from enum import Enum

class ResourceAttributes:
    CLOUD_PROVIDER: str
    CLOUD_ACCOUNT_ID: str
    CLOUD_REGION: str
    CLOUD_AVAILABILITY_ZONE: str
    CLOUD_PLATFORM: str
    AWS_ECS_CONTAINER_ARN: str
    AWS_ECS_CLUSTER_ARN: str
    AWS_ECS_LAUNCHTYPE: str
    AWS_ECS_TASK_ARN: str
    AWS_ECS_TASK_FAMILY: str
    AWS_ECS_TASK_REVISION: str
    AWS_EKS_CLUSTER_ARN: str
    AWS_LOG_GROUP_NAMES: str
    AWS_LOG_GROUP_ARNS: str
    AWS_LOG_STREAM_NAMES: str
    AWS_LOG_STREAM_ARNS: str
    CONTAINER_NAME: str
    CONTAINER_ID: str
    CONTAINER_RUNTIME: str
    CONTAINER_IMAGE_NAME: str
    CONTAINER_IMAGE_TAG: str
    DEPLOYMENT_ENVIRONMENT: str
    DEVICE_ID: str
    DEVICE_MODEL_IDENTIFIER: str
    DEVICE_MODEL_NAME: str
    FAAS_NAME: str
    FAAS_ID: str
    FAAS_VERSION: str
    FAAS_INSTANCE: str
    FAAS_MAX_MEMORY: str
    HOST_ID: str
    HOST_NAME: str
    HOST_TYPE: str
    HOST_ARCH: str
    HOST_IMAGE_NAME: str
    HOST_IMAGE_ID: str
    HOST_IMAGE_VERSION: str
    K8S_CLUSTER_NAME: str
    K8S_NODE_NAME: str
    K8S_NODE_UID: str
    K8S_NAMESPACE_NAME: str
    K8S_POD_UID: str
    K8S_POD_NAME: str
    K8S_CONTAINER_NAME: str
    K8S_REPLICASET_UID: str
    K8S_REPLICASET_NAME: str
    K8S_DEPLOYMENT_UID: str
    K8S_DEPLOYMENT_NAME: str
    K8S_STATEFULSET_UID: str
    K8S_STATEFULSET_NAME: str
    K8S_DAEMONSET_UID: str
    K8S_DAEMONSET_NAME: str
    K8S_JOB_UID: str
    K8S_JOB_NAME: str
    K8S_CRONJOB_UID: str
    K8S_CRONJOB_NAME: str
    OS_TYPE: str
    OS_DESCRIPTION: str
    OS_NAME: str
    OS_VERSION: str
    PROCESS_PID: str
    PROCESS_EXECUTABLE_NAME: str
    PROCESS_EXECUTABLE_PATH: str
    PROCESS_COMMAND: str
    PROCESS_COMMAND_LINE: str
    PROCESS_COMMAND_ARGS: str
    PROCESS_OWNER: str
    PROCESS_RUNTIME_NAME: str
    PROCESS_RUNTIME_VERSION: str
    PROCESS_RUNTIME_DESCRIPTION: str
    SERVICE_NAME: str
    SERVICE_NAMESPACE: str
    SERVICE_INSTANCE_ID: str
    SERVICE_VERSION: str
    TELEMETRY_SDK_NAME: str
    TELEMETRY_SDK_LANGUAGE: str
    TELEMETRY_SDK_VERSION: str
    TELEMETRY_AUTO_VERSION: str
    WEBENGINE_NAME: str
    WEBENGINE_VERSION: str
    WEBENGINE_DESCRIPTION: str

class CloudProviderValues(Enum):
    ALIBABA_CLOUD: str
    AWS: str
    AZURE: str
    GCP: str

class CloudPlatformValues(Enum):
    ALIBABA_CLOUD_ECS: str
    ALIBABA_CLOUD_FC: str
    AWS_EC2: str
    AWS_ECS: str
    AWS_EKS: str
    AWS_LAMBDA: str
    AWS_ELASTIC_BEANSTALK: str
    AZURE_VM: str
    AZURE_CONTAINER_INSTANCES: str
    AZURE_AKS: str
    AZURE_FUNCTIONS: str
    AZURE_APP_SERVICE: str
    GCP_COMPUTE_ENGINE: str
    GCP_CLOUD_RUN: str
    GCP_KUBERNETES_ENGINE: str
    GCP_CLOUD_FUNCTIONS: str
    GCP_APP_ENGINE: str

class AwsEcsLaunchtypeValues(Enum):
    EC2: str
    FARGATE: str

class HostArchValues(Enum):
    AMD64: str
    ARM32: str
    ARM64: str
    IA64: str
    PPC32: str
    PPC64: str
    X86: str

class OsTypeValues(Enum):
    WINDOWS: str
    LINUX: str
    DARWIN: str
    FREEBSD: str
    NETBSD: str
    OPENBSD: str
    DRAGONFLYBSD: str
    HPUX: str
    AIX: str
    SOLARIS: str
    Z_OS: str

class TelemetrySdkLanguageValues(Enum):
    CPP: str
    DOTNET: str
    ERLANG: str
    GO: str
    JAVA: str
    NODEJS: str
    PHP: str
    PYTHON: str
    RUBY: str
    WEBJS: str
