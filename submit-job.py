import boto3
import configparser

config = configparser.ConfigParser()
config.read('dl.cfg')

CFG = {
    'KEY': config.get('AWS', 'AWS_ACCESS_KEY_ID'),
    'SECRET': config.get('AWS', 'AWS_SECRET_ACCESS_KEY'),
    'REGION': config.get('AWS', 'REGION')
}


def main():
    """ Get a running EMR cluster and submit the etl.py spark job.

        The etl.py file must have already been copied to the master node
    """
    client = boto3.client('emr',
                          aws_access_key_id=CFG["KEY"],
                          aws_secret_access_key=CFG["SECRET"],
                          region_name=CFG["REGION"]
                        )


    clusters = client.list_clusters()

    # choose a cluster that is available
    clusters = [c["Id"] for c in clusters["Clusters"] 
                if c["Status"]["State"] in ["RUNNING", "WAITING"]]

    if not clusters:
        raise Exception("No valid clusters")

    # take the first relevant cluster
    cluster_id = clusters[0]
    step_args = ["/usr/bin/spark-submit", "/tmp/etl.py"]


    action = client.add_job_flow_steps(
        JobFlowId=cluster_id,
        Steps=[
            {
                'Name': 'songplay-etl',
                'ActionOnFailure': 'CANCEL_AND_WAIT',            
                'HadoopJarStep': {
                    'Jar': 's3n://elasticmapreduce/libs/script-runner/script-runner.jar',
                    'Args': step_args
                    }
            },
            ]
        )


if __name__ == "__main__":
    main()
