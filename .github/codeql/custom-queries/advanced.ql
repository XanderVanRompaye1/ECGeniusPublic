import python

from File f, Function fun, Import imp
where f.getBaseName() in ["config.py","main.py","model_functions.py"]
select f.getBaseName() as file, fun.getName(), imp.getAnImportedModuleName()
