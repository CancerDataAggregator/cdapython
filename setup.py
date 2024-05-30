from setuptools import find_packages, setup

# We need to install the cda-client library as a dependency, and we
# currently do this by telling pip to fetch the code from the github
# repo where it lives. Configure the necessary information for inclusion
# in cdapython's dependency list (see setup_list just below).

cda_openapi_python_sdk_package_name = 'cda-client'
cda_openapi_python_sdk_github_repo_url = 'https://github.com/CancerDataAggregator/cda-service-python-client.git'
cda_openapi_python_sdk_github_repo_branch_to_use = 'code_review_testing'

cda_openapi_python_sdk_setup_list_entry = f"{cda_openapi_python_sdk_package_name}@git+{cda_openapi_python_sdk_github_repo_url}@{cda_openapi_python_sdk_github_repo_branch_to_use}"

# Enumerate all required cdapython dependencies.

setup_list = [
    
    'pandas >= 1.5.3',
    'wheel >= 0.42.0',
    'cda-client >= 1.0.0',
    'tabulate >= 0.9.0',
    #cda_openapi_python_sdk_setup_list_entry,
]

# Load a long description of the cdapython package from the README
# in the repo root.

with open( 'README.md' ) as IN:
    
    long_description = IN.read()

setup(

    # Name and version of the base package.

    name = 'cdapython',
    version = '2024.0.1',

    # Tell setup.py where the packages it's managing can be located.
    # Keep an __init__.py file in each package subdirectory (including
    # the root) that you want included in the installation.

    packages = find_packages( where='.', exclude='tests' ),

    # The 'cdapython' package can, one hopes unsurprisingly, be found
    # in the 'cdapython' subdirectory.

    package_dir = {
        'cdapython': 'cdapython'
    },

    # Tell setup.py to include specific file types beyond the default (.py).
    # The '' keyword in this dictionary means "for all packages affected by
    # this setup.py[, in addition to the default Python source code files,
    # include also all files whose names match the following patterns]".):

    package_data = {
        '': [ '*.lark' ]
    },

    # Minimum necessary Python framework version.

    python_requires = '>=3.8',

    # Tell pip to install all required dependencies (as enumerated in setup_list, above).

    install_requires = setup_list,

    # Decorate the package metadata with descriptive text.

    description = 'Python library wrapping an OpenAPI-generated Python SDK built to query the CDA REST API.',
    long_description = long_description,
    long_description_content_type = 'text/markdown',

    # Consider including versions of these (along with any metadata
    # not listed here that would be helpful to include).
    # 
    # platforms = [ 'POSIX', 'MacOS', 'Windows' ],
    # url = 'https://github.com/CancerDataAggregator/cdapython',
)


