import python

from File file, Function f, Comment c, Import i

where file.getBaseName() in ["app.py", "local_functions.py", "config.py","main.py","model_functions.py","snowflake_functions.py"]

select file.getBaseName(),
       i.getAnImportedModuleName(),
       f.getName(),
       c.getText()
