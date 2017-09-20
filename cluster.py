import boto3
from gevent import sleep
from worker import Worker


class Cluster(object):
    def __init__(self, ami, sgroup, region="ap-southeast-1"):
        self.ami = ami
        self.sg = sgroup
        self.ec2 = boto3.resource("ec2", region_name=region)

    def launch_instances(self, ins_type="t2.micro", ins_num=1):
        instances = self.ec2.create_instances(
            ImageId=self.ami,
            InstanceType=ins_type,
            MinCount=ins_num,
            MaxCount=ins_num,
            SecurityGroupIds=[self.sg],
            UserData=None)
        ins_ids = [ins.instance_id for ins in instances]
        while not all(self.instance_is_ready(ins_id) for ins_id in ins_ids):
            sleep(1)
        return ins_ids

    def instance_is_ready(self, ins_id):
        return self.ec2.Instance(ins_id).state["Code"] == 16

    def instance_ip(self, ins_id):
        return self.ec2.Instance(ins_id).public_dns_name

    def launch_workers(self, num, ins_type="t2.micro"):
        workers = []
        for _, ins_ips in self.launch_instances(ins_type, ins_type, num):
            workers.append(Worker.client(ins_ips))
        for worker in workers:
            print(worker.hello())
        return workers


if __name__ == "__main__":
    cluster = Cluster("ami-10bb2373", "sg-c86bc4ae")
    ins_ids = cluster.launch_instances(ins_num=2)
    for iid in ins_ids:
        print(iid, cluster.instance_ip(iid))
