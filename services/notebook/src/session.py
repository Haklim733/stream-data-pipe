#!/usr/bin/env python3
from enum import Enum
import os
from pathlib import Path
import zipfile
import tempfile
from typing import Dict, Optional
from dataclasses import dataclass
from abc import ABC, abstractmethod

from pyspark.sql import SparkSession


class SparkVersion(Enum):
    """Enum for Spark versions and connection types"""

    SPARK_CONNECT_3_5 = "spark_connect_3_5"  # Spark Connect (3.5)
    SPARK_CONNECT_4_0 = "spark_connect_4_0"  # Spark Connect (4.0+)
    SPARK_3_5 = "spark_3_5"  # Regular PySpark (3.5)
    SPARK_4_0 = "spark_4_0"  # Regular PySpark (4.0)


class SparkConfig(ABC):
    """Abstract base class for all Spark configuration classes"""

    @property
    @abstractmethod
    def config(self) -> Dict[str, str]:
        """Return a dictionary of Spark configuration parameters"""
        pass


@dataclass
class S3FileSystemConfig(SparkConfig):
    """Configuration class for S3 Filesystem settings (security/credential overrides only)"""

    endpoint: str = "minio:9000"
    region: str = "us-east-1"
    access_key: Optional[str] = "admin"
    secret_key: Optional[str] = "password"
    path_style_access: bool = True
    ssl_enabled: bool = False

    @property
    def config(self) -> Dict[str, str]:
        """Get Spark configuration dictionary for S3 Filesystem (security/credential overrides only)"""
        configs = {
            "spark.hadoop.fs.s3a.access.key": self.access_key,
            "spark.hadoop.fs.s3a.secret.key": self.secret_key,
            "spark.hadoop.fs.s3a.region": self.region,
            "spark.hadoop.fs.s3a.endpoint": f"http://{self.endpoint}",
            "spark.hadoop.fs.s3a.force.path.style": str(self.path_style_access).lower(),
            "spark.hadoop.fs.s3a.connection.ssl.enabled": str(self.ssl_enabled).lower(),
            "spark.hadoop.fs.s3a.ssl.enabled": str(self.ssl_enabled).lower(),
        }
        return configs


@dataclass
class IcebergConfig(SparkConfig):
    """Configuration class for Iceberg settings"""

    s3_config: S3FileSystemConfig
    catalog_uri: str = "http://iceberg-rest:8181"
    warehouse: str = "s3://iceberg/wh"
    catalog_type: str = "rest"
    catalog: str = "iceberg"

    @property
    def config(self) -> Dict[str, str]:
        """Get Spark configuration dictionary for Iceberg (application-specific only)"""
        configs = {
            # Application-specific Iceberg settings (server-side configs are in spark-defaults.conf)
            # Only set credentials and endpoint - let server-side handle catalog type and other settings
            "spark.sql.catalog.iceberg.s3.endpoint": f"http://{self.s3_config.endpoint}",
            "spark.sql.catalog.iceberg.s3.access-key": self.s3_config.access_key
            or "admin",
            "spark.sql.catalog.iceberg.s3.secret-key": self.s3_config.secret_key
            or "password",
            "spark.sql.catalog.iceberg.s3.region": self.s3_config.region or "us-east-1",
            "spark.sql.defaultCatalog": self.catalog,
            "spark.sql.catalog.iceberg.type": self.catalog_type,
            "spark.sql.catalog.iceberg.uri": self.catalog_uri,
            "spark.sql.catalog.iceberg.warehouse": self.warehouse,
            "spark.sql.catalog.iceberg.s3.path-style-access": str(
                self.s3_config.path_style_access
            ).lower(),
            "spark.sql.catalog.iceberg.s3.ssl-enabled": str(
                self.s3_config.ssl_enabled
            ).lower(),
        }

        return configs


@dataclass
class PerformanceConfig(SparkConfig):
    """Configuration class for Spark performance settings"""

    adaptive_query_execution: bool = True
    shuffle_partitions: int = 200
    max_partition_bytes: str = "128m"
    advisory_partition_size: str = "128m"
    skew_join_enabled: bool = True
    skewed_partition_threshold: str = "256m"
    arrow_pyspark_enabled: bool = True
    use_kryo_serializer: bool = False

    @property
    def config(self) -> Dict[str, str]:
        """Get Spark configuration dictionary for performance settings"""
        configs = {
            "spark.sql.adaptive.enabled": str(self.adaptive_query_execution).lower(),
            "spark.sql.adaptive.coalescePartitions.enabled": str(
                self.adaptive_query_execution
            ).lower(),
            "spark.sql.adaptive.skewJoin.enabled": str(self.skew_join_enabled).lower(),
            "spark.sql.adaptive.localShuffleReader.enabled": str(
                self.adaptive_query_execution
            ).lower(),
            "spark.sql.shuffle.partitions": str(self.shuffle_partitions),
            "spark.sql.files.maxPartitionBytes": self.max_partition_bytes,
            "spark.sql.adaptive.advisoryPartitionSizeInBytes": self.advisory_partition_size,
            "spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes": self.skewed_partition_threshold,
            "spark.sql.execution.arrow.pyspark.enabled": str(
                self.arrow_pyspark_enabled
            ).lower(),
            "spark.sql.execution.arrow.pyspark.fallback.enabled": str(
                self.arrow_pyspark_enabled
            ).lower(),
        }
        if self.use_kryo_serializer:
            configs["spark.serializer"] = "org.apache.spark.serializer.KryoSerializer"

        return configs


@dataclass
class Log4jConfig(SparkConfig):
    """Configuration class for Log4j2 settings with per-app support"""

    app_name: str = "App"
    enable_per_app_logging: bool = True
    log_level: str = "INFO"
    enable_console_logging: bool = True
    enable_file_logging: bool = True
    enable_hybrid_logging: bool = True
    max_file_size: str = "100MB"
    max_backup_index: int = 10
    hybrid_max_file_size: str = "200MB"
    hybrid_max_backup_index: int = 20

    @property
    def config(self) -> Dict[str, str]:
        """Get Spark configuration dictionary for Log4j2 settings"""
        configs = {}

        if self.enable_per_app_logging:
            # Determine log directory based on environment
            if os.path.exists("/opt/bitnami/spark") or os.getenv(
                "SPARK_HOME", ""
            ).startswith("/opt/bitnami"):
                # Docker environment
                log_dir = f"/opt/bitnami/spark/logs/app/{self.app_name}"
                app_log_file = f"{log_dir}/{self.app_name}-application.log"
                hybrid_log_file = f"{log_dir}/{self.app_name}-hybrid-observability.log"
            else:
                # Local environment
                log_dir = f"./spark-logs/app/{self.app_name}"
                app_log_file = f"{log_dir}/{self.app_name}-application.log"
                hybrid_log_file = f"{log_dir}/{self.app_name}-hybrid-observability.log"

            # Create log directory
            os.makedirs(log_dir, exist_ok=True)

            # Set system properties for app-specific log paths (matching log4j2.properties)
            configs.update(
                {
                    "spark.driver.extraJavaOptions": f"-Dspark.app.log.file={app_log_file} -Dspark.hybrid.log.file={hybrid_log_file}",
                    "spark.executor.extraJavaOptions": f"-Dspark.app.log.file={app_log_file} -Dspark.hybrid.log.file={hybrid_log_file}",
                }
            )
        else:
            # Use default log4j2 configuration
            configs.update(
                {
                    "spark.driver.extraJavaOptions": "",
                    "spark.executor.extraJavaOptions": "",
                }
            )

        return configs


def create_spark_session(
    spark_version: SparkVersion = SparkVersion.SPARK_CONNECT_3_5,
    app_name: Optional[str] = None,
    iceberg_config: Optional[IcebergConfig] = None,
    performance_config: Optional[PerformanceConfig] = None,
    s3_config: Optional[S3FileSystemConfig] = None,
    log4j_config: Optional[Log4jConfig] = None,
    **additional_configs,
) -> SparkSession:
    """
    Create a Spark session based on version enum with optional Iceberg configuration

    Args:
        spark_version: SparkVersion enum specifying the type of session to create
        app_name: Name of the Spark application (used as prefix for event logs)
        iceberg_config: Optional IcebergConfig for Iceberg integration (defaults to Iceberg)
        performance_config: Optional PerformanceConfig for performance tuning
        s3_config: Optional S3FileSystemConfig for S3/MinIO access without Iceberg
        log4j_config: Optional Log4jConfig for logging configuration
        **additional_configs: Additional Spark configurations (overrides defaults)

    Returns:
        SparkSession: Configured Spark session
    """
    # Auto-detect app name from __file__ if not provided
    if app_name is None:
        app_name = Path(__file__).stem or "SparkApp"

    # Use provided performance config or create a default one
    if performance_config is None:
        performance_config = PerformanceConfig()

    if s3_config is None:
        s3_config = S3FileSystemConfig()
    # If iceberg_config is not provided, use default IcebergConfig
    if iceberg_config is None:
        iceberg_config = IcebergConfig(s3_config)

    merged_configs = {
        **s3_config.config,
        **iceberg_config.config,
        **performance_config.config,
        **additional_configs,
    }

    # Create Log4jConfig with the actual app_name (only for regular Spark, not Spark Connect)
    if spark_version not in [
        SparkVersion.SPARK_CONNECT_4_0,
        SparkVersion.SPARK_CONNECT_3_5,
    ]:
        # For regular Spark, use Log4jConfig
        if log4j_config is None:
            log4j_config = Log4jConfig(app_name=app_name)
        # Start with performance configs (application-specific)
        merged_configs.update(log4j_config.config)

        log_dir = f"/opt/bitnami/spark/logs/app/{app_name}"
        if os.path.exists("/opt/bitnami/spark") or os.getenv(
            "SPARK_HOME", ""
        ).startswith("/opt/bitnami"):
            # We're in Docker environment
            try:
                os.makedirs(log_dir, exist_ok=True)
                merged_configs.update(
                    {
                        "spark.eventLog.enabled": "true",
                        "spark.eventLog.dir": f"file:///opt/bitnami/spark/logs/app/{app_name}",
                    }
                )
            except PermissionError:
                print(
                    f"⚠️  Warning: Could not create log directory {log_dir}. Event logging disabled."
                )

        # Set up event logging (only for regular Spark, not Spark Connect)
        else:
            # We're running locally, use a local log directory
            local_log_dir = f"./spark-logs/app/{app_name}"
            try:
                os.makedirs(local_log_dir, exist_ok=True)
                merged_configs.update(
                    {
                        "spark.eventLog.enabled": "true",
                        "spark.eventLog.dir": f"file://{os.path.abspath(local_log_dir)}",
                    }
                )
            except Exception as e:
                print(f"⚠️  Warning: Could not set up event logging: {e}")

    if spark_version in [
        SparkVersion.SPARK_CONNECT_4_0,
        SparkVersion.SPARK_CONNECT_3_5,
    ]:
        # For Spark Connect, use the consolidated session function
        return create_spark_connect_session(
            app_name=app_name,
            iceberg_config=iceberg_config,
            s3_config=s3_config,
            **merged_configs,
        )
    elif spark_version in [SparkVersion.SPARK_3_5, SparkVersion.SPARK_4_0]:
        return _create_pyspark_session(app_name=app_name, spark_params=merged_configs)
    else:
        raise ValueError(f"Unsupported Spark version: {spark_version}")


def _create_src_archive() -> str:
    """Create a zip archive of the src directory for Spark Connect artifacts"""
    src_dir = "src"
    if not os.path.exists(src_dir):
        raise FileNotFoundError(f"src directory not found: {src_dir}")

    # Create temporary zip file
    temp_file = tempfile.NamedTemporaryFile(suffix=".zip", delete=False)
    temp_path = temp_file.name
    temp_file.close()

    with zipfile.ZipFile(temp_path, "w", zipfile.ZIP_DEFLATED) as zipf:
        for root, dirs, files in os.walk(src_dir):
            # Skip cache and git directories
            dirs[:] = [d for d in dirs if not d.startswith("__") and d != ".git"]
            for file in files:
                if file.endswith((".py", ".json", ".yaml", ".yml")):
                    file_path = os.path.join(root, file)
                    arcname = os.path.relpath(file_path, src_dir)
                    zipf.write(file_path, arcname)

    return temp_path


def create_spark_connect_session(
    app_name: str = None,
    iceberg_config: Optional[IcebergConfig] = None,
    s3_config: Optional[S3FileSystemConfig] = None,
    add_artifacts: bool = False,
    **additional_configs,
) -> SparkSession:
    """
    Create a Spark Connect session with optional Iceberg configuration

    Args:
        app_name: Name of the Spark application
        iceberg_config: Optional IcebergConfig for Iceberg integration
        s3_config: Optional S3FileSystemConfig for S3/MinIO access
        add_artifacts: Whether to add src directory as artifacts
        **additional_configs: Additional Spark configurations

    Returns:
        SparkSession: Configured Spark Connect session
    """
    # Auto-detect app name from __file__ if not provided
    if app_name is None:
        import inspect

        try:
            # Get the calling frame
            frame = inspect.currentframe()
            while frame:
                frame = frame.f_back
                if frame and frame.f_globals.get("__file__"):
                    # Extract filename without extension
                    app_name = os.path.splitext(
                        os.path.basename(frame.f_globals["__file__"])
                    )[0]
                    break
        except Exception:
            app_name = "SparkConnectApp"

    # Start with additional configs
    spark_params = {**additional_configs}

    # Add S3A filesystem configuration if provided
    if s3_config:
        spark_params.update(s3_config.config)

    # Add Iceberg configuration if provided
    if iceberg_config:
        spark_params.update(iceberg_config.config)

    # Add artifacts flag if requested
    if add_artifacts:
        spark_params["spark.connect.add.artifacts"] = "true"

    # Note: Spark Connect doesn't support event logging configuration
    # Event logs are handled by the Spark Connect server itself
    # We only create the directory structure for consistency
    log_dir = f"/opt/bitnami/spark/logs/app/{app_name}"
    if os.path.exists("/opt/bitnami/spark") or os.getenv("SPARK_HOME", "").startswith(
        "/opt/bitnami"
    ):
        # We're in Docker environment
        try:
            os.makedirs(log_dir, exist_ok=True)
        except PermissionError:
            print(f"⚠️  Warning: Could not create log directory {log_dir}.")
    else:
        # We're running locally, use a local log directory
        local_log_dir = f"./spark-logs/app/{app_name}"
        try:
            os.makedirs(local_log_dir, exist_ok=True)
        except Exception as e:
            print(f"⚠️  Warning: Could not create log directory: {e}")

    return _create_spark_connect_session(app_name, spark_params)


def _create_spark_connect_session(
    app_name: str,
    spark_params: Dict[str, str] = None,
    add_artifacts: bool = False,
) -> SparkSession:
    """
    Internal function to create a Spark Connect session (3.5+)

    Args:
        app_name: Name of the Spark application
        spark_params: Dictionary of Spark configuration parameters
        add_artifacts: Whether to add src directory as artifacts

    Returns:
        SparkSession: Configured Spark Connect session
    """
    if spark_params is None:
        spark_params = {}

    spark_connect_server = os.getenv("SPARK_CONNECT_SERVER", "sc://localhost:15002")

    builder = SparkSession.builder.appName(app_name).remote(spark_connect_server)

    # Apply additional configurations
    for key, value in spark_params.items():
        builder = builder.config(key, value)

    spark = builder.getOrCreate()

    # Add src directory as artifact for Spark Connect (optional)
    # Check both the parameter and the config flag
    should_add_artifacts = (
        add_artifacts
        or spark_params.get("spark.connect.add.artifacts", "false").lower() == "true"
    )

    if should_add_artifacts:
        try:
            src_archive = _create_src_archive()
            spark.addArtifact(src_archive, pyfile=True)
            # Clean up the temporary file after adding to Spark
            os.unlink(src_archive)
        except Exception as e:
            print(f"Warning: Could not add src directory as artifact: {e}")

    return spark


def _get_iceberg_jars() -> str:
    """Get the Iceberg JARs for the classpath"""
    # Check if we're in Docker environment
    if os.path.exists("/opt/bitnami/spark"):
        spark_home = "/opt/bitnami/spark"
    else:
        # We're running locally, Iceberg JARs should be available via Spark Connect
        return ""

    iceberg_jars = [
        f"{spark_home}/jars/iceberg-spark-runtime-3.5_2.12-1.9.1.jar",
        f"{spark_home}/jars/iceberg-aws-bundle-1.9.1.jar",
        f"{spark_home}/jars/spark-avro_2.12-3.5.6.jar",
    ]
    return ",".join([jar for jar in iceberg_jars if os.path.exists(jar)])


def _create_pyspark_session(
    app_name: str, spark_params: Dict[str, str]
) -> SparkSession:
    """Create a regular PySpark session (3.5)"""
    # Create event log directory in app folder

    builder = SparkSession.builder.appName(app_name)

    # Load spark-defaults.conf configurations (only if in Docker environment)
    spark_defaults_path = "/opt/bitnami/spark/conf/spark-defaults.conf"
    if os.path.exists(spark_defaults_path):
        try:
            with open(spark_defaults_path, "r") as f:
                for line in f:
                    line = line.strip()
                    if line and not line.startswith("#") and " " in line:
                        key, value = line.split(" ", 1)
                        # Only set if not already in spark_params (spark_params takes precedence)
                        if key not in spark_params:
                            builder = builder.config(key, value)
        except Exception as e:
            print(f"⚠️  Warning: Could not read spark-defaults.conf: {e}")

    # Add Iceberg JARs to classpath
    iceberg_jars = _get_iceberg_jars()
    if iceberg_jars:
        builder = builder.config("spark.jars", iceberg_jars)

    # Apply additional configurations (these override spark-defaults.conf)
    for key, value in spark_params.items():
        builder = builder.config(key, value)

    # Set master for regular PySpark
    builder = builder.master("spark://spark-master:7077")

    return builder.getOrCreate()
