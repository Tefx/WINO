import boto3
from gevent import sleep
from worker import Worker
from monitor import Monitor

user_data = """#!/bin/bash
git -C /opt/wino pull
/opt/wino/monitor.py /opt/wino/ > /wino.log
"""


class Cluster(object):
    def __init__(self, ami, sgroup, region="ap-southeast-1"):
        self.ami = ami
        self.sg = sgroup
        self.ec2 = boto3.resource("ec2", region_name=region)

    def launch_vms(self, vm_type="t2.micro", vm_num=1):
        vms = self.ec2.create_instances(
            ImageId=self.ami,
            InstanceType=vm_type,
            MinCount=vm_num,
            MaxCount=vm_num,
            KeyName="research",
            SecurityGroupIds=[self.sg],
            UserData=user_data)
        vids = [vm.instance_id for vm in vms]
        while not all(self.vm_is_ready(vid) for vid in vids):
            sleep(1)
        return vids

    def vm_is_ready(self, vid):
        return self.ec2.Instance(vid).state["Code"] == 16

    def vm_ip(self, vid):
        return self.ec2.Instance(vid).public_dns_name

    def existing_vms(self, num=20):
        vms = []
        for vm in self.ec2.instances.filter(Filters=[{
                "Name":
                "instance-state-name",
                'Values': ["running"]
        }, {
                "Name": "image-id",
                'Values': [self.ami]
        }]):
            print("Existing VM found:", vm.instance_id)
            vms.append(vm.instance_id)
            if len(vms) == num:
                break
        return vms

    def create_workers(self, num, vm_type="t2.micro"):
        vms = self.existing_vms(num)
        if len(vms) < num:
            print("{} new VMs to launch".format(num - len(vms)))
            vms.extend(self.launch_vms(vm_type, num - len(vms)))
        workers = []
        for vid in vms:
            ip = self.vm_ip(vid)
            monitor = Monitor.client(ip)
            monitor.start_worker(update=False)
            worker = Worker.client(ip)
            print("{} from worker @ {}".format(worker.hello(), ip))
            workers.append(worker)
        return workers


if __name__ == "__main__":
    cluster = Cluster("ami-fd33459e", "sg-c86bc4ae")
    cluster.create_workers(1)
