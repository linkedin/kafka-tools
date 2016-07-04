import glob
import importlib
import inspect
import os


def get_modules(base_package, base_class):
    module_list = []

    module_file_paths = glob.glob(os.path.join(base_package.__path__[0], "*.py"))
    for module_file_path in module_file_paths:
        module_filename = os.path.basename(module_file_path)
        module_name = os.path.splitext(module_filename)[0]
        if module_name.startswith("__"):
            continue

        # Import the module
        module = importlib.import_module("." + module_name, package=base_package.__name__)

        # Iterate the classes in the imported module
        for item in dir(module):
            value = getattr(module, item)
            if not value or not inspect.isclass(value) or inspect.isabstract(value):
                continue

            base_classes = inspect.getmro(value)
            if len(base_classes) == 1:
                continue
            if base_classes[1] == base_class:
                module_list.append(value)
                continue

    return list(set(module_list))
