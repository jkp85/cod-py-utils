# COD Python Utilities

This library contains helper functions for interacting with third-party APIs and external cloud services.
The main goal of `cod-py-utils` is to centralize/standardize these communal helper functions for use across the various Cup Of Data repositories.

### Installation and Use
This repo should be installed as a package within other Cup Of Data Python projects. Python 3.6+ required.
1. Change directory into the repository needing `cod-py-utils`:
```
$ cd <path_to_repo>
```
2. Activate repo's virtual environment as normal:
```
$ source venv/bin/activate
```
3. Once activated, install `cod-py-utils` from its GitHub URL (not currently on PyPi):
```
<venv>$ pip install git+https://github.com/cupofdata/cod-py-utils.git
```
4. Once installed, call the library using a normal Python import statement:
```
from cod_services.aws_utils import AWSHelper
```