import os
from pathlib import Path
import pytest
import importlib

src_package_names = ["isamples_metadata", "isb_lib", "isb_web"]
module_names: list[str] = []
p = Path(__file__)
for package_name in src_package_names:
    module_directory_path = os.path.join(p.parent.parent, package_name)
    tree = os.listdir(module_directory_path)
    for path in tree:
        if "tests" not in path and path.endswith(".py"):
            local_module_name = path.removesuffix(".py")
            full_module_name = f"{package_name}.{local_module_name}"
            module_names.append(full_module_name)


@pytest.mark.parametrize("module_name", module_names)
def test_import_modules(module_name: str):
    importlib.import_module(module_name)
