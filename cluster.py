import boto3
from gevent import sleep


class Controller(object):
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
            SecurityGroupIds=[self.sg])
        ins_ids = [ins.instance_id for ins in instances]
        while not all(self.instance_is_ready(ins_id) for ins_id in ins_ids):
            sleep(1)
        return ins_ids

    def instance_is_ready(self, ins_id):
        return self.ec2.Instance(ins_id).state["Code"] == 16


if __name__ == "__main__":
    controller = Controller("ami-10bb2373", "sg-c86bc4ae")
    print(controller.launch_instances())
