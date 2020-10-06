import collections
import copy


def dict_sanitize(data, encoding='utf-8'):
    if isinstance(data, str):
        return str(data.encode(encoding))
    elif isinstance(data, tuple):
        return data
    elif isinstance(data, collections.Mapping):
        return dict((str(key), dict_sanitize(value)) for key, value in data.items())
    elif isinstance(data, collections.Iterable):
        return type(data)(map(dict_sanitize, data))
    else:
        return data


def dict_to_tuple(dic, keys, default=None):
    return tuple(dic.get(key, default) for key in keys)


def inverse_dict(dic):
    new = {}
    for key, value in dic.iteritems():
        if isinstance(value, collections.Iterable):
            for val in value:
                new[val] = key
        else:
            new[value] = key
    return new


def deep_update_key(orig, new, key=None):
    """
    Copy new to orig
    :param orig: original dictionary
    :param new: new value (without specifying the key must be dict type)
    :param key: optional key of orig to update
    :return: orig dict updated with new value
    """
    if orig is None or not isinstance(orig, collections.Mapping):
        raise ValueError("orig must be a dict")

    if key:
        data = orig.get(key, None)
        if isinstance(data, collections.Mapping):
            orig[key] = deep_update(data, new)
        else:
            orig[key] = copy.deepcopy(new)
        return orig
    elif isinstance(orig, collections.Mapping):
        return deep_update(orig, new)
    else:
        raise ValueError("Without specifying the key new must be dict type")


def join_dicts(a, b):
    c = {}
    c.update(a)
    c.update(b)
    return c


def deep_update(origDict, newDict):
    for key, val in newDict.items():
        if isinstance(val, collections.Mapping) and val:
            defaultVal = dict(val)
            defaultVal.clear()
            orig = origDict.get(key, defaultVal)
            if isinstance(orig, collections.Mapping):
                tmp = deep_update(orig, val)
            else:
                tmp = val
            origDict[key] = tmp
        else:
            origDict[key] = newDict[key]
    return origDict


def get_child(keys, dct):
    if len(keys) > 1:
        sub_dct = dct.get(keys[0], None)
        if sub_dct is None:
            res = None
        else:
            res = get_child(keys[1:], dct[keys[0]])
        return res
    else:
        if keys[0] in dct:
            return dct[keys[0]]
        else:
            return None



if __name__ == '__main__':

    def test_deep_update():
        source = {'hello1': 1}
        overrides = {'hello2': 2}
        deep_update(source, overrides)
        assert source == {'hello1': 1, 'hello2': 2}

        source = {'hello': 'to_override'}
        overrides = {'hello': 'over'}
        deep_update(source, overrides)
        assert source == {'hello': 'over'}

        source = {'hello': {'value': 'to_override', 'no_change': 1}}
        overrides = {'hello': {'value': 'over'}}
        deep_update(source, overrides)
        assert source == {'hello': {'value': 'over', 'no_change': 1}}

        source = {'hello': {'value': 'to_override', 'no_change': 1}}
        overrides = {'hello': {'value': {}}}
        deep_update(source, overrides)
        assert source == {'hello': {'value': {}, 'no_change': 1}}

        source = {'hello': {'value': {}, 'no_change': 1}}
        overrides = {'hello': {'value': 2}}
        deep_update(source, overrides)
        assert source == {'hello': {'value': 2, 'no_change': 1}}

        source = {'k1': 1}
        overrides = {'k1': {'k2': {'k3': 3}}}
        deep_update(source, overrides)
        assert source == {'k1': {'k2': {'k3': 3}}}

        source = {'k1': [1]}
        overrides = {'k1': {'k2': {'k3': 3}}}
        deep_update(source, overrides)
        assert source == {'k1': {'k2': {'k3': 3}}}

        source = {'k1': [1]}
        overrides = {'k1': [3]}
        deep_update(source, overrides)
        assert source == {'k1': [3]}

    test_deep_update()