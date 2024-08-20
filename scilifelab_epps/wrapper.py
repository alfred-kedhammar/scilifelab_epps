import logging
import os
import sys
from datetime import datetime as dt

from genologics.config import BASEURI, PASSWORD, USERNAME
from genologics.entities import Process
from genologics.lims import Lims

from scilifelab_epps.epp import upload_file


def epp_decorator(file: str):
    script_name: str = os.path.basename(file).split(".")[0]

    def _epp_decorator(script_main):
        def epp_wrapper(args):
            """General wrapper for EPP scripts."""

            timestamp = dt.now().strftime("%y%m%d_%H%M%S")

            # Set up LIMS
            lims = Lims(BASEURI, USERNAME, PASSWORD)
            lims.check_version()
            process = Process(lims, id=args.pid)

            # Name log file
            log_filename: str = (
                "_".join(
                    [
                        script_name,
                        process.id,
                        timestamp,
                        process.technician.name.replace(" ", ""),
                    ]
                )
                + ".log"
            )

            # Set up logging
            logging.basicConfig(
                filename=log_filename,
                filemode="w",
                format="%(levelname)s: %(message)s",
                level=logging.INFO,
            )

            # Start logging
            logging.info(f"Script '{script_name}' started at {timestamp}.")
            logging.info(
                f"Launched in step '{process.type.name}' ({process.id}) by {process.technician.name}."
            )
            args_str = "\n\t".join(
                [f"'{arg}': {getattr(args, arg)}" for arg in vars(args)]
            )
            logging.info(f"Script called with arguments: \n\t{args_str}")

            # Run
            try:
                script_main(args)

            except Exception as e:
                # Post error to LIMS GUI
                logging.error(str(e), exc_info=True)
                logging.shutdown()
                upload_file(
                    file_path=log_filename,
                    file_slot=args.log,
                    process=process,
                    lims=lims,
                )
                os.remove(log_filename)
                sys.stderr.write(str(e))
                sys.exit(2)
            else:
                logging.info("")
                logging.info("Script completed successfully.")
                logging.shutdown()
                upload_file(
                    file_path=log_filename,
                    file_slot=args.log,
                    process=process,
                    lims=lims,
                )
                # Check log for errors and warnings
                log_content = open(log_filename).read()
                os.remove(log_filename)
                if "ERROR:" in log_content or "WARNING:" in log_content:
                    sys.stderr.write(
                        "Script finished successfully, but log contains errors or warnings, please have a look."
                    )
                    sys.exit(2)
                else:
                    sys.exit(0)

        return epp_wrapper

    return _epp_decorator
