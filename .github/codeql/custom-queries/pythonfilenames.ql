/**
 * @id codeql/custom-queries/pythonfilenames.ql
 * @name get filenames 
 * @description get filenames
 * @kind getting data
 * @tags empty
 *       Xander
 */

import python

from Function f, File file
where file.getExtension() = "py"
select f.getName(), file.getBaseName()