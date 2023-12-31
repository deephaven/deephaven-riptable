# Deephaven Riptable Integration

deephaven-riptable is a Python package created by Deephaven Data Labs. It supports the conversion between Riptable Datasets and Deephaven tables.

## Dev environment setup
``` shell
$ pip3 install -r requirements-dev.txt
```

## Build
``` shell
$ python3 -m build --wheel
```

## Install
``` shell
$ pip3 install dist/deephaven_riptable-0.1.0-py3-none-any.whl
```

## Run examples/data_conv.py
**The following packages must be installed before running examples: riptable, deephaven-core, and deephaven-riptable**

*Note, currently installing riptable via pip or conda most likely would fail. Please reach out to the riptable team for help.*
``` shell
$ python3 ./examples/data_conv.py
```

