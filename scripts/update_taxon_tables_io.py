#! /usr/bin/env python3

"""
Input task_broad_terra_tables.wdl (task) and PHB repo and identify task I/O that is
discrepant with downstream workflows

- Currently prints recommended changes, but staged for automated updating in place
- Does not account for variables that have the same name, but different type declarations
- Does not accomodate multiple calls to task in same workflow
"""

import os
import re
import sys
import glob
try:
    import WDL 
except ImportError:
    sys.stderr.write("ERROR: WDL Python package not found\nInstall MiniWDL\n")
    sys.exit(3)
import argparse
from collections import defaultdict


def collect_files(directory = './', filetype = '*', recursive = False):
    '''
    Inputs: directory path, file extension (no "."), recursivity bool
    Outputs: list of files with `filetype`
    If the filetype is a list, split it, else make a list of the one entry.
    Parse the environment variable if applicable. Then, obtain a clean, full 
    version of the input directory. Glob to obtain the filelist for each 
    filetype based on whether or not it is recursive.
    '''

    if type(filetype) == list:
        filetypes = filetype.split()
    else:
        filetypes = [filetype]

    directory = format_path(directory)
    filelist = []
    for filetype in filetypes:
        if recursive:
            filelist.extend( 
                glob.glob(directory + "/**/*." + filetype, recursive = recursive)
            )
        else:
            filelist.extend(
                glob.glob(directory + "/*." + filetype, recursive = recursive) 
            )

    return filelist

def eprint(*args, **kwargs):
    """Print to stderr"""
    print(*args, file=sys.stderr, **kwargs)

def expand_env_var(path):
    '''Expands environment variables by regex substitution'''

    envs = re.findall(r'\$[^/]+', path)
    for env in envs:
        path = path.replace(env, os.environ[env.replace('$','')])

    return path.replace('//','/')

def format_path(path, force_dir = False):
    '''Goal is to convert all path types to absolute path with explicit dirs'''

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

def get_io(wdl_file):
    """Get imports from WDL file"""
    wf = WDL.load(wdl_file)
    task2inputs = {}
    task2outputs = {}
    wf2inputs = {}
    wf2outputs = {}
    # This may need to be updated to accomodate other instances
    if wf.tasks:
        for task in wf.tasks:
            task2inputs[task.name] = task.inputs
            task2outputs[task.name] = task.outputs
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

def obtain_namespace_inputs(wdl_file, namespace, task):
    """Obtain the inputs of a task in a WDL file using REGEX"""
    with open(wdl_file, 'r') as raw:
        wdl_data = raw.read()
    # prepare a compiled regex to find the task and its inputs
    re_comp = re.compile(r'call\W+' + f'{namespace}\.{task}' + r'\W+{' \
                       + r'\s+input:\s+([^}]+)', re.DOTALL)
    matches = re_comp.findall(wdl_data)
    if len(list(matches)) > 1:
        eprint(f'ERROR: Multiple instances of {namespace}.{task} found in {wdl_file}' \
                + ' - this script is not equipped to handle this')
        sys.exit(4)
    elif '{' in matches[0]:
        eprint('ERROR: REGEX not equipped to handle nested "{" ' \
             + f'in {namespace}.{task} in {wdl_file}')
        sys.exit(5)
    namespace_inputs = {}
    input_list = matches[0].split('\n')
    for input in input_list:
        inp_data = input.strip().split('=')
        if len(inp_data) == 2:
            inp_name = inp_data[0].strip()
            inp_expr = inp_data[1].strip().replace(',', '')
            namespace_inputs[inp_name] = inp_expr

    return namespace_inputs

def get_downstream(foc_file, foc_info, wdl_files, 
                   repo_dir, task = 'export_taxon_tables'):
    """Get downstream dependencies of a WDL file"""
    preexisting = {}
    downstream = {}
    for wdl_file in wdl_files:
        wdl = WDL.load(wdl_file)
        wdl_dir = os.path.dirname(wdl_file)
        for wdl_import in wdl.imports:
            uri = wdl_import.uri
            uri_path = format_path(os.path.join(wdl_dir, uri))
            if uri_path == foc_file:
                eprint(f'\t{wdl_file}')
                task_io, wf_io = get_io(wdl_file)
                namespace = wdl_import.namespace
                downstream[wdl_file] = {'namespace': namespace,
                                        'outputs': wf_io['outputs'],
                                        'inputs': wf_io['inputs']}
                preexisting[wdl_file] = obtain_namespace_inputs(wdl_file, namespace, task)

    return downstream, preexisting

def get_upstream(foc_file, repo_dir):
    """Get upstream dependencies of a WDL file"""
    upstream = {}
    wdl = WDL.load(foc_file)
    wdl_dir = os.path.dirname(foc_file)
    for wdl_import in wdl.imports:
        uri = wdl_import.uri
        uri_path = format_path(os.path.join(wdl_dir, uri))
        upstream[uri_path] = wdl_import.namespace
        eprint(uri_path, wdl_import.namespace)

    return upstream

def compile_outputs(io_dict):
    """Identify the outputs to eventually 
    populate the input script's inputs"""
        # have to convert the WDL type objects to a proper string for hashing
#   input_data = {(inp.name, str(inp.type),) for inp in inputs}
    wdl2in2type = defaultdict(dict)
    wdl2out2type = defaultdict(dict)
    wdl2out_hash, wdl2in_hash = {}, {}
    wdl2namespace = {}
    for wdl_file, wdl_info in io_dict.items():
        # namespace = wdl_info['namespace']
        wdl2out_hash[wdl_file] = {}
        wdl2in_hash[wdl_file] = {}
        wdl2namespace[wdl_file] = wdl_info['namespace']
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

def print_changes(input_file, input_inputs, wdl2out_hash, 
                  wdl2namespace, task_name, preexisting, task,
                  ignored_inputs, wdl2in_hash, wdl2in2type, wdl2out2type):
    """Print the input script's new inputs"""

    # Identify and print inputs from the workflow that do not correspond
    print()
    task_in2type = defaultdict(set)
    for wdl_file, task_dict in wdl2out_hash.items():
        namespace = wdl2namespace[wdl_file]
        print(f'{wdl_file}')
        print(f'\tAdd to {namespace}.{task_name} call:')
        outputs2expr, inputs2expr = {}, {}
        for task0, outputs in task_dict.items():
            for out_name, out_var in outputs:
                outputs2expr[out_name] = out_var
        for task0, inputs in wdl2in_hash[wdl_file].items():
            for in_name, in_var in inputs:
                inputs2expr[in_name] = in_var

        # The workflow's inputs and outputs are the only acceptable inputs to the task
        acceptable_inputs = set(inputs2expr.keys()).union(set(outputs2expr.keys()))
        # Extraneous task inputs are preexisting inputs that aren't acceptable
        extra_inputs = set(preexisting[wdl_file]).difference(acceptable_inputs)
        # Only unexposed outputs are flagged for adding
        missing_inputs = set(outputs2expr.keys()).difference(set(preexisting[wdl_file]))
        # Identify the total inputs for populating the task file itself later
        actual_inputs = set(preexisting[wdl_file]).intersection(acceptable_inputs).union(
                            missing_inputs)

        for missing_var in sorted(missing_inputs):
            missing_expr = outputs2expr[missing_var]
            print(f'{missing_var} = {missing_expr}')
        print(f'\tRemove from {namespace}.{task_name} call:')
        for extra_var in sorted(extra_inputs):
            extra_expr = preexisting[wdl_file][extra_var]
            print(f'{extra_var} = {extra_expr}')

        # Crude check to obtain the type for populating the task's input 
        for in_var in list(actual_inputs):
            # Populate a list of types to ensure there aren't multiple types
            if in_var in wdl2in2type[wdl_file]:
                task_in2type[in_var].add(wdl2in2type[wdl_file][in_var])
            if in_var in wdl2out2type[wdl_file]:
                task_in2type[in_var].add(wdl2out2type[wdl_file][in_var])
        print()

    # Report if discrepant types for a given variable name
    failed_vars = [k for k, v in task_in2type.items() if len(v) > 1]
    if any(failed_vars):
        eprint('ERROR: some variables have multiple types across workflows: ')
        for failed_var in failed_vars:
            eprint(f'{failed_var} {task_in2type[failed_var]}')
        sys.exit(6)

    # Report the discrepancies for the input task

    print(input_file)
    print('\tAdd to inputs:')
    for var_name, typ in out_hash2wdl:
        if var_name not in input_inputs:
            # make all inputs optional
            if not typ.endswith('?'):
                typ = typ + '?'
            print(f'{typ} {var_name}')
    
    extraneous_inputs = input_inputs.difference(set(x[0] for x in out_hash2wdl))
    extraneous_inputs = extraneous_inputs.difference(ignored_inputs)
    print('\tRemove from inputs:')
    for inp in sorted(extraneous_inputs):
        print(f'{inp}')
    # add removing section

            
def main(input_file, repo_dir, update = False, task_name = 'export_taxon_tables',
         ignored_inputs = {'cpu', 'memory', 'disk_size', 'docker'}):
    """Main function:
    Compile inputs from input_file
    ID downstream dependencies
    Extract I/O from downstream dependencies
    Extract preexisting inputs to input_file task from dependency
    Report missing inputs from input_file
    Report extra inputs from input_file
    Report missing inputs in dependencies' task calls
    Report extra inputs in dependencies' task calls
    """
    # Check if repo is a git repo
    if not check_repo_head(repo_dir):
        eprint("ERROR: Repo directory is not a git repository")
        sys.exit(1)

    # Get inputs and outputs of focal WDL file
    task_io, wf_io = get_io(input_file)
    if task_io and wf_io:
        eprint("ERROR: this script does not support WDL files with both tasks and workflows")
        sys.exit(2)
    elif task_io:
        wdl_info = task_io
    else:
        wdl_info = wf_io

    input_inputs = set(x.name for x in wdl_info['inputs'][task_name])

    # Collect all WDL files in repo and ID dependencies
    wdl_files_prep = set(collect_files(repo_dir, 'wdl', recursive = True))
    wdl_files = sorted(wdl_files_prep.difference({input_file}))
    eprint('Downstream dependencies:')
    downstream_io, preexisting_inputs = get_downstream(input_file, wdl_info, 
                                                     wdl_files, repo_dir)
    if not downstream_io:
        eprint("No downstream dependencies found")
        sys.exit(3)

#    eprint('Upstream dependencies:') 
 #   upstreams = get_upstream(input_file, repo_dir)

    wdl2out_hash, wdl2namespace, wdl2in_hash, wdl2in2type, wdl2out2type \
        = compile_outputs(downstream_io)

    # Print the new inputs for the focal WDL file
    if not update:
        print_changes(input_file, input_inputs, wdl2out_hash, 
                      wdl2namespace, task_name, preexisting_inputs, task_name,
                      ignored_inputs, wdl2in_hash, wdl2in2type, wdl2out2type)

def cli():
    parser = argparse.ArgumentParser(description = "Sync WDL inputs/outputs")
    parser.add_argument("-i", "--input", help = "WDL file to sync", required = True)
    parser.add_argument("-r", "--repo", help = "Base repo dir containing WDL files", required = True)
   # parser.add_argument("-u", "--update", action = "store_true", 
                     #   help = "Update the focal WDL and dependencies with new I/O")
    args = parser.parse_args()

    main(format_path(args.input), format_path(args.repo)) #, args.downstream)


if __name__ == '__main__':
    cli()
    sys.exit(0)