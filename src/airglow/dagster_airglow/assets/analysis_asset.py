# Standard library imports
import os
import tempfile
import traceback
from datetime import datetime, timedelta
from typing import Dict, Any, Optional, List

# Third-party imports
import dagster as dg
from dagster import EnvVar, MetadataValue
from dagster_mysql import MySQLResource
from dagster_ncsa import S3ResourceNCSA

# Local imports
from airglow import FPIprocess, fpiinfo
from airglow.exceptions import NoSkyImagesError
from airglow.GitHubIssueHandler import SiteIssueManager, IssueType
from airglow.github_config import github_config


class AnalysisConfig(dg.Config):
    """Model representing the output from the unzip_chunked_archive asset."""

    site: str = "uao"
    year: str = "2025"
    observation_date: str = "20250429"
    fpi_data_path: str = "fpi/minime05/uao/2025/20250429"
    cloud_cover_path: str = "cloudsensor/uao/2025"
    fail_asset_on_all_errors: bool = False  # Set to True if you want the asset to fail when all processing fails


class DagsterErrorHandler:
    """
    Enhanced error handler that integrates GitHubIssueHandler with Dagster logging.
    Mirrors the functionality from Sorter_OSN.py's _handle_processing_error and _handle_warning.
    """
    
    # Extended error type mapping
    EXTENDED_ERROR_LABELS = {
        "NoLaserImagesError": "error:no_laser_images",
        "NoSkyImagesError": "error:no_sky_images", 
        "BadLaserError": "error:laser_centerfinding",
        "LaserProcessingError": "error:laser_general",
        "InstrumentProcessingError": "error:instrument", 
        "BadDopplerReferenceError": "error:bad_doppler_reference",
        "ProcessingError": "error:processing_general",
        "FileNotFoundError": "error:file_not_found",
        "PermissionError": "error:permission_denied",
        "ValueError": "error:invalid_value",
        "KeyError": "error:missing_key",
        "IndexError": "error:index_out_of_range",
        "TypeError": "error:type_mismatch",
        "RuntimeError": "error:runtime"
    }
    
    def __init__(self, context: dg.AssetExecutionContext, site: str):
        self.context = context
        self.site = site
        self.logger = context.log
        
        # Initialize GitHub issue handler
        try:
            self.issue_manager = SiteIssueManager(
                token=github_config.github_token,
                repo_name=github_config.github_repo
            )
        except Exception as e:
            self.logger.error(f"Failed to initialize GitHub issue manager: {e}")
            self.issue_manager = None
    
    def _get_site_assignees(self) -> List[str]:
        """
        Get the list of GitHub usernames to assign issues for this site.
        Returns a list of GitHub usernames based on site configuration.
        """
        try:
            # Get site information from fpiinfo
            site_info = fpiinfo.get_site_info(self.site)
            
            # Check if assignees are defined in the site info
            if isinstance(site_info, dict) and 'github_assignees' in site_info:
                assignees = site_info['github_assignees']
                if isinstance(assignees, list):
                    return assignees
                elif isinstance(assignees, str):
                    return [assignees]  # Single assignee as string
            
            # Fallback: try to get from email addresses and convert to GitHub usernames
            # This assumes you have a mapping of email -> GitHub username
            if isinstance(site_info, dict) and 'email' in site_info:
                emails = site_info['email']
                if isinstance(emails, list):
                    # You could maintain a mapping of email -> GitHub username
                    # For now, we'll extract the username part before @ and hope it matches
                    github_usernames = []
                    for email in emails:
                        if '@' in email:
                            username = email.split('@')[0]
                            # You might want to validate these usernames exist on GitHub
                            github_usernames.append(username)
                    return github_usernames
                elif isinstance(emails, str) and '@' in emails:
                    return [emails.split('@')[0]]
            
        except Exception as e:
            self.logger.warning(f"Could not get assignees for site {self.site}: {e}")
        
        # Return empty list if no assignees found
        return []
    
    def _handle_processing_error(self, error: Exception, metadata: Dict[str, Any]) -> None:
        """
        Handle processing errors with enhanced logging and GitHub issue creation.
        Mirrors the _handle_processing_error function from Sorter_OSN.py
        """
        error_info = {
            "error_type": type(error).__name__,
            "error_message": str(error),
            "traceback": traceback.format_exc(),
            "timestamp": datetime.utcnow().isoformat(),
            "asset_name": self.context.asset_key.to_user_string(),
            "run_id": self.context.run_id,
            "site": self.site,
            **metadata
        }
        
        # Log the error with Dagster
        self.logger.error(
            f"Processing error in {error_info['asset_name']} for site {self.site}: {error_info['error_message']}",
            extra={
                "dagster_metadata": {
                    "error_details": MetadataValue.json(error_info),
                    "error_traceback": MetadataValue.text(error_info["traceback"]),
                    "site": MetadataValue.text(self.site),
                    "error_type": MetadataValue.text(error_info["error_type"])
                }
            }
        )
        
        # Create GitHub issue if manager is available
        if self.issue_manager:
            try:
                # Determine error type for proper labeling
                error_type = type(error).__name__
                
                # Get assignees for this site
                assignees = self._get_site_assignees()
                if assignees:
                    self.logger.info(f"Assigning GitHub issue to: {', '.join(assignees)}")
                
                # Use our extended error labels if the original doesn't have it
                if hasattr(self.issue_manager, 'ERROR_LABELS') and error_type not in self.issue_manager.ERROR_LABELS:
                    if error_type in self.EXTENDED_ERROR_LABELS:
                        # Temporarily add the error type to the issue manager's labels
                        self.issue_manager.ERROR_LABELS[error_type] = self.EXTENDED_ERROR_LABELS[error_type]
                        self.logger.info(f"Added missing error type '{error_type}' to issue manager")
                
                # Create the issue with assignees
                issue = self.issue_manager.handle_processing_issue(
                    site_id=self.site,
                    message=str(error),
                    category=IssueType.ERROR,
                    error_type=error_type,
                    additional_context={
                        "Asset": error_info["asset_name"],
                        "Run ID": error_info["run_id"],
                        "Timestamp": error_info["timestamp"],
                        "Traceback": error_info["traceback"],
                        "Error Type": error_type,
                        **metadata
                    }
                )
                
                # Assign the issue to the appropriate users
                if issue and assignees:
                    try:
                        issue.edit(assignees=assignees)
                        self.logger.info(f"Successfully assigned issue to: {', '.join(assignees)}")
                    except Exception as assign_error:
                        self.logger.warning(f"Failed to assign issue to {assignees}: {assign_error}")
                
                self.logger.info(f"Created GitHub issue for error: {error_type} at site {self.site}")
                    
            except Exception as github_error:
                self.logger.error(f"Failed to create GitHub issue: {github_error}")
                # Log the original error details since GitHub issue creation failed
                self.logger.error(f"Original error was: {type(error).__name__}: {str(error)}")
                self.logger.error(f"Error context: {metadata}")
    
    def _handle_warning(self, warning_message: str, metadata: Dict[str, Any]) -> None:
        """
        Handle warnings with enhanced logging and optional GitHub issue creation.
        Mirrors the _handle_warning function from Sorter_OSN.py
        """
        warning_info = {
            "warning_message": warning_message,
            "timestamp": datetime.utcnow().isoformat(),
            "asset_name": self.context.asset_key.to_user_string(),
            "run_id": self.context.run_id,
            "site": self.site,
            **metadata
        }
        
        # Log the warning with Dagster
        self.logger.warning(
            f"Processing warning in {warning_info['asset_name']} for site {self.site}: {warning_message}",
            extra={
                "dagster_metadata": {
                    "warning_details": MetadataValue.json(warning_info),
                    "site": MetadataValue.text(self.site)
                }
            }
        )
        
        # Create GitHub issue if manager is available
        if self.issue_manager:
            try:
                # Get assignees for this site
                assignees = self._get_site_assignees()
                if assignees:
                    self.logger.info(f"Assigning GitHub warning issue to: {', '.join(assignees)}")
                
                # Create the warning issue
                issue = self.issue_manager.handle_processing_issue(
                    site_id=self.site,
                    message=warning_message,
                    category=IssueType.WARNING,
                    additional_context={
                        "Asset": warning_info["asset_name"],
                        "Run ID": warning_info["run_id"],
                        "Timestamp": warning_info["timestamp"],
                        **metadata
                    }
                )
                
                # Assign the issue to the appropriate users
                if issue and assignees:
                    try:
                        issue.edit(assignees=assignees)
                        self.logger.info(f"Successfully assigned warning issue to: {', '.join(assignees)}")
                    except Exception as assign_error:
                        self.logger.warning(f"Failed to assign warning issue to {assignees}: {assign_error}")
                
                self.logger.info(f"Created GitHub issue for warning at site {self.site}")
                
            except Exception as github_error:
                self.logger.error(f"Failed to create GitHub issue for warning: {github_error}")


def get_instrument_info(site, year, doy):
    """Get instrument information for the given site and date."""
    # Get date from year and doy
    nominal_dt = datetime(year, 1, 1) + timedelta(days=doy - 1)

    # Get the instrument name at this site
    instr_name = fpiinfo.get_instr_at(site, nominal_dt)[0]

    # Import the site information
    site_name = fpiinfo.get_site_of(instr_name, nominal_dt)

    # Create "minime05_uao_20130729" string
    datestr = nominal_dt.strftime("%Y%m%d")
    instrsitedate = instr_name + "_" + site_name + "_" + datestr

    # What tags we expect at this site
    site_info = fpiinfo.get_site_info(site)
    expected_tags = site_info['expected_tags']
    
    return instr_name, site_name, datestr, instrsitedate, expected_tags


def download_fpi_data(
    context: dg.AssetExecutionContext,
    s3: S3ResourceNCSA,
    site,
    year,
    datestr,
    target_dir,
    fpi_data_path: str,
    cloud_cover_path: str,
    bucket_prefix_dir: str = "",
):
    """Download FPI data files from cloud storage."""

    created_files = []

    # Get the instrument name for the site
    instr_name = get_instrument_info(site, year, int(datestr[-3:]))[0]

    # Download FPI data
    context.log.info(f"Downloading FPI data for {fpi_data_path}")

    files = s3.list_files(EnvVar("DEST_BUCKET").get_value(), fpi_data_path, "")

    for f in files:
        relative_path = os.path.dirname(f[len(bucket_prefix_dir):])
        relative_file = f[len(bucket_prefix_dir):]

        local_path = os.path.join(target_dir, relative_path)
        os.makedirs(local_path, exist_ok=True)

        local_file = os.path.join(target_dir, relative_file)
        s3.get_client().download_file(
            Bucket=EnvVar("DEST_BUCKET").get_value(), Key=f, Filename=local_file
        )

        created_files.append(local_file)

    # Download cloud sensor data
    context.log.info(f"Downloading cloud sensor data for {site} on {datestr}")
    context.log.info(f"Cloud cover path: {cloud_cover_path}")
    files = s3.list_files(EnvVar("DEST_BUCKET").get_value(), cloud_cover_path, ".txt")

    for f in files:
        relative_path = os.path.dirname(f[len(bucket_prefix_dir):])
        relative_file = f[len(bucket_prefix_dir):]

        local_path = os.path.join(target_dir, relative_path)
        os.makedirs(local_path, exist_ok=True)

        local_file = os.path.join(target_dir, relative_file)

        s3.get_client().download_file(
            Bucket=EnvVar("DEST_BUCKET").get_value(), Key=f, Filename=local_file
        )

        created_files.append(local_file)

    return created_files, instr_name


def upload_results(
    context: dg.AssetExecutionContext,
    s3: S3ResourceNCSA,
    local_dir: str,
    object_prefix: str,
):
    """Upload the results to cloud storage."""
    if not os.path.exists(local_dir):
        context.log.info(f"Local directory {local_dir} does not exist.")
        return

    # Upload the results to the cloud storage
    bucket_name = EnvVar("DEST_BUCKET").get_value()
    s3_client = s3.get_client()
    context.log.info(f"Uploading results to /{object_prefix}")

    # Ensure the prefix ends with a slash if it's not empty
    if object_prefix and not object_prefix.endswith("/"):
        object_prefix += "/"

    uploaded_files = []

    # Walk through the directory
    for root, dirs, files in os.walk(local_dir):
        for file in files:
            # Get the full local path
            local_path = os.path.join(root, file)

            # Calculate the relative path from the base directory
            relative_path = os.path.relpath(local_path, local_dir)

            # Create the S3 object key with the prefix
            s3_key = object_prefix + relative_path

            try:
                # Upload the file
                s3_client.upload_file(local_path, bucket_name, s3_key)
                uploaded_files.append(local_path)
                context.log.info(
                    f"Uploaded {local_path} to s3://{bucket_name}/{s3_key}"
                )
            except Exception as e:
                print(f"Error uploading {local_path}: {str(e)}")

    return uploaded_files


def analyze_data(
    context: dg.AssetExecutionContext,
    config: AnalysisConfig,
    s3: S3ResourceNCSA,
    mysql: MySQLResource,
) -> str:
    """
    Analyze the data and return the analysis result.
    Enhanced with comprehensive error handling and GitHub issue creation.
    """
    # Initialize error handler
    error_handler = DagsterErrorHandler(context, config.site)
    
    # Track processing statistics
    processing_stats = {
        "total_processed": 0,
        "successful": 0,
        "errors": 0,
        "warnings": 0,
        "sky_line_tags_processed": []
    }
    
    # Perform some analysis on the data
    observation_date = config.observation_date
    date_obj = datetime.strptime(observation_date, "%Y%m%d")
    doy = date_obj.timetuple().tm_yday
    year = int(config.year)

    # Get date string from year and day of year
    instr_name, site_name, datestr, instrsitedate, expected_tags = get_instrument_info(
        config.site, year, doy
    )

    context.log.info("Instrument name: %s", instr_name)
    context.log.info("Site name: %s", site_name)
    context.log.info("Date string: %s", datestr)
    context.log.info("Instrument site date: %s", instrsitedate)
    context.log.info(f"Expected sky tags: {expected_tags}")

    # Create a temporary directory context manager
    with tempfile.TemporaryDirectory() as temp_dir:
        try:
            # Define all paths using the temporary directory
            fpi_dir = os.path.join(temp_dir, "fpi/")
            bw_dir = os.path.join(temp_dir, "cloudsensor/")
            x300_dir = os.path.join(temp_dir, "templogs/x300/")
            results_stub = os.path.join(temp_dir, "test/results/")
            madrigal_stub = os.path.join(temp_dir, "test/madrigal/")
            share_stub = os.path.join(temp_dir, "share/")
            temp_plots_stub = os.path.join(temp_dir, "temporary_plots/")

            # Make sure all directories exist
            for directory in [
                fpi_dir,
                bw_dir,
                x300_dir,
                results_stub,
                madrigal_stub,
                share_stub,
                temp_plots_stub,
            ]:
                os.makedirs(directory, exist_ok=True)

            # Download data with error handling
            try:
                download_fpi_data(
                    context=context,
                    s3=s3,
                    site=config.site,
                    year=year,
                    datestr=observation_date,
                    target_dir=temp_dir,
                    fpi_data_path=config.fpi_data_path,
                    cloud_cover_path=config.cloud_cover_path,
                )
            except Exception as download_error:
                error_handler._handle_processing_error(
                    download_error,
                    {
                        "step": "data_download",
                        "fpi_data_path": config.fpi_data_path,
                        "cloud_cover_path": config.cloud_cover_path
                    }
                )
                raise

            # Enhanced processing loop with comprehensive error handling
            # This replaces the original try/except block around line 216
            for sky_line_tag in expected_tags:#["X", "XR", "XG"]:
                processing_stats["total_processed"] += 1
                processing_stats["sky_line_tags_processed"].append(sky_line_tag)
                
                context.log.info(f"Processing sky line tag: {sky_line_tag}")
                
                # Context information for this specific processing step
                step_context = {
                    "sky_line_tag": sky_line_tag,
                    "instrument": instr_name,
                    "year": year,
                    "doy": doy,
                    "datestr": datestr,
                    "processing_step": "FPIprocess.process_instr"
                }
                
                try:
                    # Call FPIprocess.process_instr with comprehensive error handling
                    warning = FPIprocess.process_instr(
                        instr_name,
                        year,
                        doy,
                        mysql=mysql,
                        sky_line_tag=sky_line_tag,
                        fpi_dir=fpi_dir,
                        bw_dir=bw_dir,
                        send_to_madrigal=True,
                        send_to_website=True,
                        x300_dir=x300_dir,
                        results_stub=results_stub,
                        madrigal_stub=madrigal_stub,
                        share_stub=share_stub,
                        temp_plots_stub=temp_plots_stub,
                    )
                    
                    # Handle any warnings returned by the processing function
                    if warning:
                        processing_stats["warnings"] += 1
                        error_handler._handle_warning(
                            warning_message=str(warning),
                            metadata=step_context
                        )
                    
                    # Upload results if processing was successful
                    try:
                        results_path = EnvVar("RESULTS_PATH").get_value()
                        upload_results(
                            context, s3, results_stub, f"{results_path}/{year}"
                        )

                        summary_path = EnvVar("SUMMARY_IMAGES_PATH").get_value()
                        upload_results(
                            context, s3, temp_plots_stub, f"{summary_path}/{year}"
                        )

                        madrigal_path = EnvVar("MADRIGAL_PATH").get_value()
                        upload_results(
                            context, s3, madrigal_stub, f"{madrigal_path}/{year}"
                        )
                        
                        processing_stats["successful"] += 1
                        context.log.info(f"Successfully processed and uploaded results for {sky_line_tag}")
                        
                    except Exception as upload_error:
                        # Handle upload errors separately - processing succeeded but upload failed
                        error_handler._handle_processing_error(
                            upload_error,
                            {**step_context, "error_location": "results_upload"}
                        )
                        # Don't fail the entire processing for upload errors
                        processing_stats["warnings"] += 1
                        
                except NoSkyImagesError as no_sky_error:
                    # This is expected for some sky line tags - treat as warning
                    processing_stats["warnings"] += 1
                    warning_message = f"No {sky_line_tag} sky images found for {instr_name} on {datestr}"
                    context.log.warning(warning_message)
                    
                    error_handler._handle_warning(
                        warning_message=warning_message,
                        metadata=step_context
                    )
                    
                except Exception as processing_error:
                    # Handle any other processing errors
                    processing_stats["errors"] += 1
                    
                    # Get the error type name for proper labeling
                    error_type = type(processing_error).__name__
                    context.log.error(f"Processing error in {sky_line_tag}: {error_type} - {str(processing_error)}")
                    
                    error_handler._handle_processing_error(
                        processing_error,
                        step_context
                    )
                    
                    # Decide whether to continue or fail - continue processing other tags
                    context.log.warning(f"Continuing processing despite error in {sky_line_tag}: {error_type}")
                    continue
            
            # Log final processing statistics
            context.log.info(f"Processing completed. Stats: {processing_stats}")
            
            # Handle final processing results
            if processing_stats["errors"] > 0:
                if processing_stats["successful"] == 0:
                    # All processing failed
                    warning_message = f"All processing failed: {processing_stats['errors']} errors occurred across all sky line tags"
#                    error_handler._handle_warning(
#                        warning_message=warning_message,
#                        metadata={
#                            "final_stats": processing_stats,
#                            "severity": "critical",
#                            "all_tags_failed": True
#                        }
#                    )
#                    
                    if config.fail_asset_on_all_errors:
                        # Fail the asset if configured to do so
                        raise RuntimeError(f"All processing failed. {processing_stats['errors']} errors occurred.")
                    else:
                        # Don't fail the asset, but return error status
                        context.log.error(warning_message)
                        return f"failed_all_processing_{processing_stats['errors']}_errors"
#                else:
#                    # Partial success - log warning but don't fail
#                    error_handler._handle_warning(
#                        warning_message=f"Partial processing failure: {processing_stats['errors']} errors, {processing_stats['successful']} successful",
#                        metadata={"final_stats": processing_stats}
#                    )
            
        except Exception as main_error:
            # Handle any unexpected errors at the top level
            error_handler._handle_processing_error(
                main_error,
                {
                    "error_location": "main_processing_loop",
                    "processing_stats": processing_stats,
                    "instrument": instr_name,
                    "year": year,
                    "doy": doy
                }
            )
            
            # Re-raise the error to fail the asset
            raise

    return "ok"


@dg.asset(
    name="analyze_data_pipeline",
    deps=["unzip_chunked_archive"],
)
def analyze_data_pipeline(
    context: dg.AssetExecutionContext,
    s3: S3ResourceNCSA,
    mysql: MySQLResource,
) -> str:
    """
    Pipeline asset that analyzes the data and returns the analysis result.
    Enhanced with comprehensive error handling and GitHub integration.
    """
    # Convert the metadata produced by the unzip_chunked_archive asset to an
    # AnalysisConfig object. Only use records from this run to avoid
    # cross-contamination
    materialization_records = context.instance.get_records_for_run(
        run_id=context.run_id,
        of_type=dg.DagsterEventType.ASSET_MATERIALIZATION
    ).records

    # Index by asset key so we can find the one we want in multi-step DAG
    materialization_dict = {
        record.asset_key.to_python_identifier(): record.asset_materialization.metadata
        for record in materialization_records
    }

    upstream_metadata = materialization_dict['unzip_chunked_archive']['analysis_config'].data

    analysis_config = AnalysisConfig(**upstream_metadata)
    context.log.info(f"Analysis config: {analysis_config}")
    return analyze_data(context, analysis_config, s3, mysql)


@dg.asset(
    name="reanalyze_data",
)
def reanalyze_data(
    context: dg.AssetExecutionContext,
    config: AnalysisConfig,
    s3: S3ResourceNCSA,
    mysql: MySQLResource,
) -> str:
    """
    Run the analysis again without needing to extract the data again.
    Enhanced with comprehensive error handling and GitHub integration.
    """
    return analyze_data(context, config, s3, mysql)