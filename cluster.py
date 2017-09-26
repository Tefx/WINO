import boto3
from gevent import pool, sleep
from worker import Worker
from monitor import Monitor


class Cluster(object):
    def __init__(self, ami, sgroup, region="ap-southeast-1", pgroup=None):
        self.ami = ami
        self.sg = sgroup
        self.pg = pgroup
        self.ec2 = boto3.resource("ec2", region_name=region)

    def launch_vms(self, vm_type="t2.micro", vm_num=1):
        placement = {"GroupName": self.pg} if self.pg else None
        vms = self.ec2.create_instances(
            ImageId=self.ami,
            InstanceType=vm_type,
            MinCount=vm_num,
            MaxCount=vm_num,
            KeyName="research",
            SecurityGroupIds=[self.sg],
            Placement=placement)
        vids = [vm.instance_id for vm in vms]
        while not all(self.vm_is_ready(vid) for vid in vids):
            sleep(1)
        return vids

    def vm_is_ready(self, vid):
        return self.ec2.Instance(vid).state["Code"] == 16

    def vm_ip(self, vid):
        return self.ec2.Instance(vid).public_dns_name

    def vm_private_ip(self, vid):
        return self.ec2.Instance(vid).private_ip_address

    def existing_vms(self, vm_type, num=20):
        vms = []
        for vm in self.ec2.instances.filter(Filters=[{
                "Name":
                "instance-state-name",
                'Values': ["running"]
        }, {
                "Name": "image-id",
                'Values': [self.ami]
        }, {
                "Name": "instance-type",
                "Values": [vm_type]
        }]):
            print("Existing VM found:", vm.instance_id)
            vms.append(vm.instance_id)
            if len(vms) == num:
                break
        return vms

    def create_vms(self, num, vm_type):
        vms = self.existing_vms(vm_type, num)
        if len(vms) < num:
            print("{} new VMs to launch".format(num - len(vms)))
            vms.extend(self.launch_vms(vm_type, num - len(vms)))
        return vms

    def get_monitor_from_vid(self, vid):
        print("Connecting to monitor@{}".format(vid))
        monitor = Monitor.client(self.vm_ip(vid))
        monitor.start_worker(update=True)
        return monitor

    def create_monitors(self, num, vm_type):
        return pool.Group().map(self.get_monitor_from_vid,
                                self.create_vms(num, vm_type))

    def get_worker_from_vid(self, vid):
        ip = self.vm_ip(vid)
        monitor = self.get_monitor_from_vid(vid)
        worker = Worker.client(ip, keep_alive=False)
        worker.private_ip = self.vm_private_ip(vid)
        return worker

    def create_workers(self, num, vm_type="t2.micro"):
        return pool.Group().map(self.get_worker_from_vid,
                                self.create_vms(num, vm_type))


if __name__ == "__main__":
    cluster = Cluster("ami-500b7d33", "sg-c86bc4ae")
    worker = cluster.create_workers(1)[0]
    print(worker.hello())
