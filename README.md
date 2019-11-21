# Verssa Automation Script

This script will automatically run CyVerse apps based on files uploaded to the /incoming directory. It is modularized to run different scripts based on file names.

Currently a number of paths and components are hard-coded inside of automation.py, so please be aware if you are forking for your own purposes.

Also included in this directory is the validation script. This validation script handles all subdirectories of the phantom_echoes_MEV1 folder and assigns meta-data upon validation. The validation script does not support modules

## Installation

Use the package manager [pip](https://pip.pypa.io/en/stable/) to install the requirements.

```bash
pip install requirements.txt
```

## Usage

```bash
python automate.py
```

```bash
python validation.py
```
## Adding modules
Modules need to be named after their corresponding file-type. For example: Data.qmg -> qmg.py.

For an example of how a module works please see qmg.py and automation.py.

Each module is passed the following via args: 
1. obj -> the IRODs object
2. logger -> the logger (ex. logger.info(""))
3. auth_headers -> Auth headers for making terrain calls
4. db -> the simple python object db to keep track of running jobs.
## Contributing
Pull requests are welcome. For major changes, please open an issue first to discuss what you would like to change.
