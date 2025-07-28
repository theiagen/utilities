#!/usr/bin/env python3
import argparse
import os
import logging
import subprocess
import tempfile
import sys
import shutil

def parse_args():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(description="Concatenate barcode files. Existing output files will be overwritten.")
    parser.add_argument("input_dir", help="Input directory containing barcode files")
    parser.add_argument("output_dir", nargs="?", default=".", help="Output directory for concatenated files")
    parser.add_argument("--file_extension", default=".fastq.gz", help="File extension to concatenate")
    parser.add_argument("--output_prefix", default="", help="Prefix for output files")
    parser.add_argument("-r", "--recursive", action="store_true", help="Process subdirectories recursively")
    parser.add_argument("--verbose", action="store_true", help="Enable verbose output")
    parser.add_argument("--dry_run", action="store_true", help="Show commands without executing")
    parser.add_argument("--log_file", default="concatenate_barcodes.log", help="Log file path")
    parser.add_argument("--gcp", action="store_true", help="Enable Google Cloud Storage mode")
    parser.add_argument("--temp_dir", default=None, help="Temporary directory for GCS files")
    parser.add_argument("--keep_temp_files", action="store_true", help="Keep temporary files after processing")
    return parser.parse_args()

def is_gcs_path(path):
    """Check if a path is a valid Google Cloud Storage path"""
    return path and path.startswith("gs://")

def run_shell_cmd(cmd, verbose=False):
    """Run a shell command and return its output"""
    if verbose:
        logging.debug("Running: {}".format(cmd))
    
    try:
        result = subprocess.run(cmd, shell=True, check=True, 
                              stdout=subprocess.PIPE, stderr=subprocess.PIPE, 
                              universal_newlines=True)
        return result.stdout.strip()
    except subprocess.CalledProcessError as e:
        logging.error("Command failed: {}".format(cmd))
        logging.error("Error: {}".format(e.stderr))
        return None

def list_gcs_subdirectories(gcs_path, verbose=False):
    """List subdirectories in a Google Cloud Storage path"""
    if not gcs_path.endswith('/'):
        gcs_path += '/'
    
    cmd = "gcloud storage ls {}".format(gcs_path)
    output = run_shell_cmd(cmd, verbose)
    
    return [line.strip() for line in output.split('\n') if line.strip()] if output else []

def process_directory(input_path, output_dir, name, args):
    """Process a single directory by concatenating its files"""
    # Build output filename
    prefix = args.output_prefix + "_" if args.output_prefix else ""
    
    if args.gcp:
        if not output_dir.endswith('/'):
            output_dir += '/'
    
    output_file = os.path.join(output_dir, "{}{}.all{}".format(prefix, name, args.file_extension))
    
    if args.gcp:        
        return handle_gcs_files(input_path, output_file, args)
    else:
        cmd = "cat {} > {}".format(os.path.join(input_path, '*' + args.file_extension), output_file)
        
        if args.dry_run:
            logging.info("Dry run: would execute: {}".format(cmd))
            return True    
        elif args.verbose:
            logging.debug("Executing: {}".format(cmd))
        
        os.system(cmd)
        return True

def handle_gcs_files(input_path, output_file, args):
    """Process files from Google Cloud Storage"""
    # In dry run mode, just log what would happen and return
    if args.dry_run:
        logging.info("Dry run: would process GCS files from {} to {}".format(input_path, output_file))
        logging.info("Dry run: would create temporary directory")
        logging.info("Dry run: would list files with: gcloud storage ls {}".format(input_path))
        logging.info("Dry run: would download matching files, concatenate them, and upload to {}".format(output_file))
        return True
        
    # Real execution below this point
    if args.temp_dir is None:
        temp_dir = tempfile.mkdtemp()
    else:
        temp_dir = args.temp_dir
    
    os.makedirs(temp_dir, exist_ok=True)
        
    if args.verbose:
        logging.debug("Using temp directory: {}".format(temp_dir))
    
    try:
        if not input_path.endswith('/'):
            input_path += '/'
        cmd = "gcloud storage ls {}*".format(input_path)
        
        if args.verbose:
            logging.debug("Listing GCS files: {}".format(cmd))
        
        file_list = run_shell_cmd(cmd, args.verbose)
        if not file_list:
            logging.warning("No files found in {}".format(input_path))
            return False
        
        gcs_files = [f.strip() for f in file_list.split('\n') 
                   if f.strip() and f.strip().endswith(args.file_extension)]
        
        if not gcs_files:
            logging.warning("No files matching {} found in {}".format(args.file_extension, input_path))
            return False
        
        local_output = os.path.join(temp_dir, os.path.basename(output_file))
        open(local_output, 'w').close()
        
        for gcs_file in gcs_files:
            cat_cmd = "gcloud storage cat {} >> {}".format(gcs_file, local_output)
            if args.verbose:
                logging.debug("Appending file from GCS: {}".format(cat_cmd))
            run_shell_cmd(cat_cmd, args.verbose)
        
        if args.verbose:
            logging.debug("Uploading to GCS: {} -> {}".format(local_output, output_file))
        return run_shell_cmd("gcloud storage cp {} {}".format(local_output, output_file), args.verbose) is not None
    finally:
        if os.path.exists(temp_dir) and not args.keep_temp_files:
            shutil.rmtree(temp_dir)
            if args.verbose:
                logging.debug("Removed temp directory: {}".format(temp_dir))

def main():
    args = parse_args()
    
    log_level = logging.DEBUG if args.verbose else logging.INFO
    logging.basicConfig(
        filename=args.log_file, 
        level=log_level,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    
    if args.verbose:
        logging.debug("Arguments: {}".format(vars(args)))

    if args.dry_run:
        logging.info("Performing dry run - no files will be modified")
    
    if args.gcp:
        try:
            run_shell_cmd("gcloud storage --help", args.verbose)
        except Exception:
            logging.error("gcloud storage command not available. Please install Google Cloud SDK.")
            sys.exit(1)
        
        if not is_gcs_path(args.input_dir):
            logging.error("Input must be a GCS path (gs://bucket/path) when using --gcp")
            sys.exit(1)
        
        if not is_gcs_path(args.output_dir):
            logging.error("Output must be a GCS path (gs://bucket/path) when using --gcp")
            sys.exit(1)
        
        logging.info("Using Google Cloud Storage mode: {} -> {}".format(args.input_dir, args.output_dir))
    
    if args.recursive:
        logging.info("Processing in recursive mode (processing each subdirectory)")
        
        if args.gcp:
            subdirs = list_gcs_subdirectories(args.input_dir, args.verbose)
            for subdir_path in subdirs:
                subdir_name = os.path.basename(subdir_path.rstrip('/'))
                process_directory(subdir_path, args.output_dir, subdir_name, args)
        else:
            for subdir in os.listdir(args.input_dir):
                subdir_path = os.path.join(args.input_dir, subdir)
                if os.path.isdir(subdir_path):
                    process_directory(subdir_path, args.output_dir, subdir, args)
    else:
        logging.info("Processing in non-recursive mode (concatenating files in input directory)")
        
        # Extract directory name for output file
        dir_name = os.path.basename(args.input_dir.rstrip('/').split('/')[-1]) if args.gcp else \
                   os.path.basename(os.path.normpath(args.input_dir))
            
        process_directory(args.input_dir, args.output_dir, dir_name, args)

    logging.info("Concatenation completed successfully")

if __name__ == "__main__":
    main()
