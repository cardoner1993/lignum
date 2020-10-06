def index_containing_substring(theList, substring):
    for i, s in enumerate(theList):
        if substring in s:
            return i
    return -1


def from_lower_camel_case_to_lower_snake_case(camelCaseWord):
    underscoreWord = ''
    for l in camelCaseWord:
        if l.isupper() or l.isdigit():
            underscoreWord+='_'+l.lower()
        else:
            underscoreWord+=l
    return underscoreWord


def from_lower_snake_case_to_lower_camel_case(underscoreWord):
    camelCaseWord = ''
    toUp = False
    for l in underscoreWord:
        if l!='_':
            if not toUp:
                camelCaseWord+=l
            else:
                camelCaseWord += l.upper()
                toUp = False
        else:
            toUp = True
    return camelCaseWord


def adapt_columns_to_db_standards(inputDF, dbType = 'sqlDB'):
    if dbType == 'sqlDB':
        renameDict = {column: from_lower_camel_case_to_lower_snake_case(column) for column in inputDF.columns}
    return inputDF.rename(columns=renameDict)


def adapt_columns_from_db_standards(inputDF, dbType = 'sqlDB'):
    if dbType == 'sqlDB':
        renameDict = {column: from_lower_snake_case_to_lower_camel_case(column) for column in inputDF.columns}
    return inputDF.rename(columns=renameDict)


def get_n_tabs(n):
    return ''.join(['\t'] * (n))


def pretty_print_as_str(o, rec=0):

    if isinstance(o, dict):
        s = ''
        for k, v in o.items():
            s += "\n%s'%s': %s" % (get_n_tabs(rec+1), k, pretty_print_as_str(v, rec+1)) + ', '
        if o:
            s = s[:-2]
        return "%s{%s\n%s}" % (get_n_tabs(rec), s, get_n_tabs(rec))
    if isinstance(o, list):
        s = ''
        for e in o:
            s += '\n' + pretty_print_as_str(e, rec+1) + ', '
        if o:
            s = s[:-2]
        return '%s[%s\n%s]' % (get_n_tabs(rec), s, get_n_tabs(rec))
    if isinstance(o, str):
        return "'%s'" % o
    return str(o)