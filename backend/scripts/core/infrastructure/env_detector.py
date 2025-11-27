import os
import boto3
from utils.logger import setup_logger

logger = setup_logger(__name__)

def is_running_on_aws() -> bool:
    """
    Detects if the code is running in an AWS environment (Lambda, Glue, or EC2).
    Returns True if AWS environment is detected, False otherwise.
    """
    try:
        session = boto3.Session()
        region = session.region_name or os.getenv("AWS_REGION")
        if not region:
            logger.warning("AWS region not found in boto3 session or environment variables.")
            return False

        sts = session.client("sts", region_name=region)
        account_id = sts.get_caller_identity().get("Account")

        is_aws_env = any([
            os.getenv("AWS_LAMBDA_FUNCTION_NAME"),
            os.getenv("GLUE_VERSION"),
            os.getenv("AWS_EXECUTION_ENV"),
            bool(account_id)
        ])

        return is_aws_env
    except Exception as e:
        logger.warning(f"Failed to detect AWS environment: {e}")

    return False