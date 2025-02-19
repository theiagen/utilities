#! /usr/bin/env python3

"""
Input task_broad_terra_tables.wdl (task) and PHB repo and identify task I/O that is
discrepant with downstream workflows. Propose updates to the task I/O and documentation.

- Currently writes recommended changes, but staged for automated updating in place
- Does not account for variables that have the same name, but different type declarations
- Does not accomodate multiple calls to task in same workflow
- Staged for remote runs, but loading remote WDL files with WDL is currently non-functional,
    - Remove required from -i and -r, uncomment other arguments to initialize remote function
"""

import os
import re
import sys
import glob
import logging
try:
    import WDL 
except ImportError:
    raise ImportError("ERROR: WDL Python package not found\nInstall MiniWDL\n")
import requests
import argparse
from io import StringIO
from collections import defaultdict

logging.basicConfig(level = logging.DEBUG,
                    format = '%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def set_wdl_paths():
    """Set dependency paths relative to remote repo"""
    source = 'tasks/utilities/data_export/task_export_taxon_table.wdl'
    dependencies = ['workflows/theiaprok/wf_theiaprok_fasta.wdl',
                    'workflows/theiaprok/wf_theiaprok_illumina_pe.wdl',
                    'workflows/theiaprok/wf_theiaprok_illumina_se.wdl',
                    'workflows/theiaprok/wf_theiaprok_ont.wdl']
    return source, dependencies

def remote_load(url):
    """Read WDL file from remote link - May need to be modified because WDL.load does
    not accept a string/StringIO object"""
    response = requests.get(url)
    response.raise_for_status()
    wdl_text = response.text
    return wdl_text

def collect_files(directory = './', filetype = '*', recursive = False):
    """
    Inputs: directory path, file extension (no "."), recursivity bool
    Outputs: list of files with `filetype`
    If the filetype is a list, split it, else make a list of the one entry.
    Parse the environment variable if applicable. Then, obtain a clean, full 
    version of the input directory. Glob to obtain the filelist for each 
    filetype based on whether or not it is recursive.
    """

    if type(filetype) == list:
        filetypes = filetype.split()
    else:
        filetypes = [filetype]

    directory = format_path(directory)
    filelist = []
    for filetype in filetypes:
        if recursive:
            filelist.extend(glob.glob(directory + "/**/*." + filetype, 
                                      recursive = recursive))
        else:
            filelist.extend(glob.glob(directory + "/*." + filetype, 
                                      recursive = recursive))

    return filelist

def expand_env_var(path):
    """Expands environment variables by regex substitution"""

    envs = re.findall(r'\$[^/]+', path)
    for env in envs:
        path = path.replace(env, os.environ[env.replace('$','')])

    return path.replace('//','/')

def format_path(path, force_dir = False):
    """Convert all path types to absolute path with explicit directory ending"""
    if path:
        path = os.path.expanduser(path)
        path = expand_env_var(path)
        if force_dir:
            if not path.endswith('/'):
                path += '/'
        else:
            if path.endswith('/'):
                if not os.path.isdir(path):
                    path = path[:-1]
            else:
                if os.path.isdir(path):
                    path += '/'
            if not path.startswith('/'):
                path = os.getcwd() + '/' + path
        path = path.replace('/./', '/')
        while '/../' in path:
            path = re.sub(r'[^/]+/\.\./(.*)', r'\1', path)
    return path

def check_repo_head(repo_dir):
    """Check if repo directory is a git repo"""
    if os.path.isdir(f'{repo_dir}/.git'):
        return True
    else:
        return False

def get_io(wdl_file, local = False):
    """Get inputs and outputs of a WDL file.
    Currently only suitable for WDL files with either tasks or workflows"""
    if local:
        wf = WDL.load(wdl_file)
    else:
        wf_tmp = remote_load(wdl_file)
        wf = WDL.load(wf_tmp)
    task2inputs = {}
    task2outputs = {}
    wf2inputs = {}
    wf2outputs = {}
    # Populate task inputs
    if wf.tasks:
        for task in wf.tasks:
            task2inputs[task.name] = task.inputs
            task2outputs[task.name] = task.outputs
    # Populate workflow inputs
    if wf.workflow:
        wf2inputs[wf.workflow.name] = wf.workflow.inputs
        wf2outputs[wf.workflow.name] = wf.workflow.outputs

    #x_info = {'inputs': {'<task/wf_name>': [<input1>, <input2>, ...], ...},
    task_info, wf_info = {}, {}
    if task2inputs or task2outputs:
        task_info = {'inputs': task2inputs, 'outputs': task2outputs}
    if wf2inputs or wf2outputs:
        wf_info = {'inputs': wf2inputs, 'outputs': wf2outputs}

    return task_info, wf_info

def obtain_namespace_inputs(wdl_file, namespace, task, local = False):
    """Obtain the inputs of a task in a WDL file w/o WDL library.
    WDL must abide by Theiagen PHB code contribution guidelines"""
    # Read the WDL file
    if local:
        with open(wdl_file, 'r') as raw:
            wdl_data = raw.read()
    else:
        wdl_data = remote_load(wdl_file)

    # prepare a compiled regex to find the task and its inputs,
    call_comp = re.compile(r'call\W+' + f'{namespace}\.{task}')
    # prepare parsing variables
    parse_block = False
    parse_input = False
    nested_brackets = 0
    closed_brackets = 0
    matches = []
    for line in wdl_data.split('\n'):
        if not parse_block:
            if call_comp.search(line) is not None:
                parse_block = True
                nested_brackets += 1
        if parse_block:
            # capture the data for the task call
            # identify when input parsing begins
            if not parse_input:
                # DOES NOT account for any inputs within the first input line
                if 'input:' in line:
                    parse_input = True
                    matches.append([])
            else:
                # Capture the input data
                matches[-1].append(line)
                # Account for nestedness, DOES NOT exclude brackets in strings
                if '{' in line:
                    nested_brackets += 1
                if '}' in line:
                    closed_brackets += 1
        # identify when a call is complete while accounting for nestedness
        if nested_brackets == closed_brackets and parse_input:
            # Reset parsing variables
            parse_block = False
            parse_input = False
            nested_brackets = 0
            closed_brackets = 0
                
    # Only one instance of the task should be found
    if len(matches) > 1:
        raise AttributeError(f'ERROR: Multiple instances of {namespace}.{task} found in {wdl_file}' \
                + ' - this script is not equipped to handle this')

    # Parse the input variables and expressions
    namespace_inputs = {}
    for input_ in matches[0]:
        inp_data = input_.strip().split('=')
        if len(inp_data) == 2:
            inp_name = inp_data[0].strip()
            inp_expr = inp_data[1].strip().replace(',', '')
            namespace_inputs[inp_name] = inp_expr
        else:
            # check if parsing a mapping
            map_data = input_.strip().split(':')
            if len(map_data) == 2:
                map_name = map_data[0].strip().replace('"', '').replace("'", '')
                map_expr = map_data[1].strip().replace(',', '')
                namespace_inputs[map_name] = map_expr

    return namespace_inputs

def get_downstream_remote(foc_file, foc_info,
                          dependencies, task = 'export_taxon_table'):
    """Parse downstream dependencies of a WDL file remotely.
    Currently non-functional due to WDL.load not accepting a string"""
    preexisting = {}
    downstream = {}
    for wdl_file in dependencies:
        wdl_txt = remote_load(wdl_file)
        wdl = WDL.load(StringIO(wdl_txt))
        for wdl_import in wdl.imports:
            logging.info(f'\t{wdl_file}')
            task_io, wf_io = get_io(wdl_file)
            namespace = wdl_import.namespace
            downstream[wdl_file] = {'namespace': namespace,
                                    'outputs': wf_io['outputs'],
                                    'inputs': wf_io['inputs']}
            preexisting[wdl_file] = obtain_namespace_inputs(wdl_file, namespace, task)

    return downstream, preexisting

def get_downstream_local(foc_file, downstream_wdls, 
                         task = 'export_taxon_table'):
    """Get and parse downstream dependencies of a WDL file"""
    preexisting = {}
    downstream = {}

    for wdl_file in downstream_wdls:
        wdl = WDL.load(wdl_file)
        wdl_dir = os.path.dirname(wdl_file)
        for wdl_import in wdl.imports:
            uri = wdl_import.uri
            uri_path = format_path(os.path.join(wdl_dir, uri))
            # Check if the focal file is imported by the downstream file
            if uri_path == foc_file:
                logger.info(f'\t{wdl_file}')
                task_io, wf_io = get_io(wdl_file, local = True)
                namespace = wdl_import.namespace
                # Get I/O from the downstream file
                downstream[wdl_file] = {'namespace': namespace,
                                        'outputs': wf_io['outputs'],
                                        'inputs': wf_io['inputs']}
                # Get the inputs to the task call in the downstream file
                preexisting[wdl_file] = obtain_namespace_inputs(wdl_file, namespace, 
                                                                task, local = True)

    return downstream, preexisting

def compile_downstream_io(io_dict):
    """Identify the downstream I/O to eventually populate the input task's inputs"""
    wdl2in2type = defaultdict(dict)
    wdl2out2type = defaultdict(dict)
    wdl2out_hash, wdl2in_hash = {}, {}
    wdl2namespace = {}
    for wdl_file, wdl_info in io_dict.items():
        wdl2out_hash[wdl_file] = {}
        wdl2in_hash[wdl_file] = {}
        wdl2namespace[wdl_file] = wdl_info['namespace']
        # Multiple workflows may be present in a single WDL file
        for task1, outputs in wdl_info['outputs'].items():
            wdl2out_hash[wdl_file][task1] = []
            # have to convert the WDL type objects to a proper string for hashing
            for out in outputs:
                wdl2out_hash[wdl_file][task1].append((out.name, out.expr,))
                wdl2out2type[wdl_file][out.name] = str(out.type).replace('?', '')
            wdl2out_hash[wdl_file][task1] = sorted(wdl2out_hash[wdl_file][task1])
        for task1, inputs in wdl_info['inputs'].items():
            wdl2in_hash[wdl_file][task1] = []
            # have to convert the WDL type objects to a proper string for hashing
            for in_ in inputs:
                wdl2in2type[wdl_file][in_.name] = str(in_.type).replace('?', '')
                wdl2in_hash[wdl_file][task1].append((in_.name, in_.expr,))
            wdl2in_hash[wdl_file][task1] = sorted(wdl2in_hash[wdl_file][task1])

    return wdl2out_hash, wdl2namespace, wdl2in_hash, wdl2in2type, wdl2out2type

def output_changes(wdl2out_hash,
                  wdl2namespace, task_name, preexisting,
                  nonmapped_inputs, ignored_inputs, ignored_outputs, 
                  wdl2in_hash, wdl2in2type, wdl2out2type, mapping_name,
                  file_obj = sys.stdout):
    """Output calls to input task's input changes, the input task's I/O changes, and
    documentation updates to input changes"""

    # Identify and write inputs from the workflow that do not correspond
    # Collect the types for each variable name to check and refer to later
    task_in2type_set = defaultdict(set)
    # Collect the files that potential focal task inputs are found in
    input2files = defaultdict(list)
    for wdl_file, task_dict in wdl2out_hash.items():
        namespace = wdl2namespace[wdl_file]
        outputs2expr, inputs2expr = {}, {}
        # Compile the variable names and expressions for the outputs and inputs
        for outputs in task_dict.values():
            for out_name, out_var in outputs:
                outputs2expr[out_name] = out_var
                input2files[out_name].append(wdl_file)
        for inputs in wdl2in_hash[wdl_file].values():
            for in_name, in_var in inputs:
                inputs2expr[in_name] = in_var
                input2files[in_name].append(wdl_file)

        # The workflow's inputs and outputs are the only acceptable inputs to the task
        acceptable_inputs_prep = set(inputs2expr.keys()).union(set(outputs2expr.keys()))
        # Add special exceptions
        acceptable_inputs = acceptable_inputs_prep.union(ignored_inputs).union(nonmapped_inputs)
        # Extraneous task inputs are preexisting inputs that aren't acceptable
        extra_inputs = set(preexisting[wdl_file]).difference(acceptable_inputs)
        # Only unexposed outputs are flagged for adding
        missing_inputs_prep = set(outputs2expr.keys()).difference(set(preexisting[wdl_file]))
        missing_inputs = missing_inputs_prep.difference(ignored_outputs)
        missing_mappings = missing_inputs.difference(nonmapped_inputs)
        missing_nonmapped = missing_inputs.intersection(nonmapped_inputs)
        # Identify the total inputs for populating the task file itself later
        accepted_preexisting = set(preexisting[wdl_file]).intersection(acceptable_inputs)
        actual_inputs = accepted_preexisting.union(missing_inputs)

        # Write the proposed changes to the file
        file_obj.write(f'!! {wdl_file}\n')
        if missing_inputs:
            if missing_nonmapped:
                file_obj.write(f'\t!! Add directly to {namespace}.{task_name} call:\n')
                for missing_var in sorted(missing_nonmapped):
                    missing_expr = outputs2expr[missing_var]
                    file_obj.write(f'{missing_var} = {missing_expr},\n')
            if missing_mappings:
                file_obj.write(f'\t!! Add to {mapping_name} mapping in {namespace}.{task_name} call:\n')
                for missing_var in sorted(missing_mappings):
                    missing_expr = outputs2expr[missing_var]
                    file_obj.write(f'{missing_var}: {missing_expr},\n')
        if extra_inputs:
            file_obj.write(f'\n\t!! Remove from {namespace}.{task_name} call:\n')
            for extra_var in sorted(extra_inputs):
                file_obj.write(f'{extra_var}\n')

        # Crude check to obtain the type for populating the task's input 
        for in_var in list(actual_inputs):
            # Populate a list of types to ensure there aren't multiple types
            if in_var in wdl2in2type[wdl_file]:
                task_in2type_set[in_var].add(wdl2in2type[wdl_file][in_var])
            if in_var in wdl2out2type[wdl_file]:
                task_in2type_set[in_var].add(wdl2out2type[wdl_file][in_var])
        file_obj.write('\n')

    # Report if discrepant types for a given variable name
    failed_vars = [k for k, v in task_in2type_set.items() if len(v) > 1]
    if any(failed_vars):
        logger.error('some variables have multiple types across workflows: ')
        for failed_var in failed_vars:
            logger.error(f'{failed_var} {task_in2type_set[failed_var]}')
        raise ValueError
            
def main(input_file, downstream_wdls, repo_dir, out_file, 
         task_name = 'export_taxon_table',
         ignored_inputs = {'cpu', 'memory', 'disk_size', 'docker'},
         ignored_outputs = {'taxon_table_status'}, 
         nonmapped_inputs = {'terra_project', 'terra_workspace', 
                             'gambit_predicted_taxon', 'columns_to_export',
                             'taxon_table', 'samplename'},
         mapping_name = 'columns_to_export',
         remote = False):
    """Main function:
    Compile inputs from input_file
    ID downstream dependencies
    Extract I/O from downstream dependencies
    Extract preexisting inputs to input_file task from dependency
    Report task call changes for downstream dependencies
    Report input changes for input_file
    Report documentation changes for input_file inputs
    """

    # Get inputs and outputs of focal WDL file
    task_io, wf_io = get_io(input_file, local = bool(repo_dir))
    if task_io and wf_io:
        raise AttributeError("ERROR: this script does not support WDL files with both tasks and workflows")
    elif task_io:
        wdl_info = task_io
    else:
        wdl_info = wf_io

    logger.info('Identifying downstream dependencies:')
    if not remote:
        # Collect all WDL files in local repo and ID dependencies
        wdls_prep = set(collect_files(repo_dir, 'wdl', recursive = True))
        wdl_files = sorted(wdls_prep.difference({input_file}))
        downstream_io, preexisting_inputs = get_downstream_local(input_file, 
                                                                 wdl_files)
        downstream_wdls = sorted(downstream_io.keys())
    else:
        downstream_io, preexisting_inputs = get_downstream_remote(input_file, wdl_info,
                                                                  downstream_wdls)
        
    if not downstream_io:
        raise FileNotFoundError("No downstream dependencies found")

    # Compile the downstream I/O for easier parsing
    wdl2out_hash, wdl2namespace, wdl2in_hash, wdl2in2type, wdl2out2type \
        = compile_downstream_io(downstream_io)

    # Write the new inputs for the focal WDL file
    with open(out_file, 'w') as out_obj:
        output_changes(wdl2out_hash, 
                      wdl2namespace, task_name, preexisting_inputs,
                      nonmapped_inputs, ignored_inputs, ignored_outputs, 
                      wdl2in_hash, wdl2in2type, wdl2out2type,
                      mapping_name, file_obj = out_obj)
    
def cli():
    base_repo_url = f'https://raw.githubusercontent.com/theiagen/public_health_bioinformatics/'
    parser = argparse.ArgumentParser(description = "Sync task_export_taxon_table.wdl inputs/outputs " \
                                     + "with downstream dependencies.")
#    parser.add_argument("-b", "--branch", default = 'main', 
 #                       help = 'Remote git branch for remote runs; DEFAULT: "main"')
  #  parser.add_argument("-u", "--url", help = f'Remote git URL; DEFAULT: {base_repo_url}',
   #                     default = base_repo_url)
    parser.add_argument("-i", "--input", required = True,
                        help = '[-r] Local task_export_taxon_table.wdl; Requires -r')
    parser.add_argument("-r", "--repo", required = True,
                        help = "[-i] Local git repo dir for local runs; Requires -i")
    args = parser.parse_args()

    if not args.input and not args.repo:
        raise AttributeError('ERROR: remote run is not implemented, use -i and -r for local')
       # remote = True
        #   repo_uri = args.url + args.branch + '/'
    #    rel_source_task, rel_dependencies = set_wdl_paths()
     #   source_task = f'{repo_uri}{rel_source_task}'
      #  dependencies = [f'{repo_uri}{x}' for x in rel_dependencies]
    elif not args.input or not args.repo:
        raise AttributeError('ERROR: -i and -r are required together')
    else:
        remote = False
        source_task = format_path(args.input)
        repo_uri = format_path(args.repo)
        # Check if repo is a git repo for local run
        if not check_repo_head(repo_uri):
            raise FileNotFoundError("ERROR: Repo directory is not a git repository")
        dependencies = []

    out_file = format_path('./update_taxon_tables_io.txt')
    main(source_task, dependencies, repo_uri, out_file, remote = remote)

if __name__ == '__main__':
    cli()
    sys.exit(0)