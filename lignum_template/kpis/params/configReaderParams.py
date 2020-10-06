# Config for reader. Use unique index if the index columns has to be unique also. If the column doesn't have to be uniq
# use index instead
# 'example': {
#         'schema': 'proves',
#         'groupers': ['local', 'quarter'],
#         'aggregators': ['sum'],
#         'variables': ['value'],
#         'columns': ['local', 'quarter', 'value_sum'],
#         'types': ['character', 'quarter', 'float'],
#         'unique_index': ['local','quarter'],
#         'index': opc1 ['local','quarter'] opc2 [['local'], ['quarter']] use this instead of unique_index if the index
#         doesn't have to be unique
#     },

configReader = {
    'monthly_trend': {
        'schema': 'proves',
        'groupers': ['local', 'month'],
        'aggregators': ['sum'],
        'variables': ['value'],
        'columns': ['local', 'year', 'month', 'value_sum'],
        'types': ['character', 'year', 'month', 'float'],
        'primary_key_flag': [False, False, False, False],
        'nullable_flag': [False, False, False, False],
        'unique_index': ['local', 'month']
    },
    'quarter_metrics': {
        'schema': 'proves',
        'groupers': ['local', 'quarter'],
        'aggregators': ['sum'],
        'variables': ['value'],
        'columns': ['local', 'quarter', 'value_sum'],
        'types': ['character', 'quarter', 'float'],
        'unique_index': ['local', 'quarter']
     },
    'daily_trend': {
        'schema': 'proves',
        'groupers': ['local', 'day'],
        'aggregators': ['sum'],
        'variables': ['value'],
        'columns': ['local', 'month', 'day', 'value_sum'],
        'types': ['character', 'month', 'date', 'float'],
        'unique_index': ['local', 'day']
    },
    'anual_metrics': {
        'schema': 'proves',
        'groupers': ['local', 'year'],
        'aggregators': ['sum'],
        'variables': ['value'],
        'columns': ['local', 'year', 'value_sum'],
        'types': ['character', 'year', 'float'],
        'unique_index': ['local', 'year']
    },
    'monthly_metrics': {
        'schema': 'proves',
        'groupers': ['local', 'month'],
        'aggregators': ['sum'],
        'variables': ['value'],
        'columns': ['local', 'month', 'value_sum'],
        'types': ['character', 'month', 'float'],
        'unique_index': ['local', 'month']
    },
    'weekly_trend': {
        'schema': 'proves',
        'groupers': ['local', 'quarter', 'week_year'],
        'aggregators': ['sum', 'mean'],
        'variables': ['value'],
        'columns': ['local', 'quarter', 'week_year', 'value_sum'],
        'types': ['character', 'quarter', 'week', 'float'],
        'unique_index': ['local', 'quarter', 'week_year']
    }
}