#!/usr/bin/python
# -*- coding:u8 -*-
__author__ = 'alexandr'

import pycassa
from pycassa.types import CompositeType,UTF8Type,LongType
from pycassa.util import OrderedDict
import re
from django.utils.encoding import smart_str

class CassandraWrapper(pycassa.ColumnFamily):
    def __init__(self,pool,name,**kwargs):
        super(CassandraWrapper,self).__init__(pool,name,**kwargs)

    def insert(self,key,value,**kwargs):
        key = self.prepare_key(key)
        value = self.prepare_dict(value)
        return super(CassandraWrapper,self).insert(key,value,**kwargs)

    def get(self,key,**kwargs):
        key =  self.prepare_key(key)
        if self.super:
            if 'super_column' in kwargs:
                kwargs['super_column']=self.convert_type(kwargs['super_column'],self._super_column_name_class)
                if 'columns' in kwargs:
                    kwargs['columns']=map(lambda value:self.convert_type(value,self._column_name_class),kwargs['columns'])
            else:
                kwargs['columns']=map(lambda value:self.convert_type(value,self._super_column_name_class),kwargs['columns'])
        else:
            kwargs['columns']=map(lambda value:self.convert_type(value,self._column_name_class),kwargs['columns'])
        return super(CassandraWrapper,self).get(key,**kwargs)

    def prepare_key(self,key):
        return self.convert_type(key,self.key_validation_class)

    def prepare_dict(self,_dict,is_super=False,ordered=False):
        result = {}
        for key,value in _dict.items():
            if is_super:
                result[self.convert_type(key,self._column_name_class)]=self.convert_type(value)
            else:
                if type(value) is not (dict or OrderedDict):
                    result[self.convert_type(key,self._column_name_class)]=self.convert_type(value)
                else:
                    result[self.convert_type(key,self._super_column_name_class)]=self.prepare_dict(value,True,ordered)
        if ordered:
            return OrderedDict(result)
        return result

    def convert_type(self,value,c_type=None):
        # get value's type_name
        if not isinstance(c_type, basestring):
            try:
                c_type = c_type.__name__
            except AttributeError:
                c_type = c_type.__class__.__name__
        # convert to long or error
        if c_type == 'LongType':
            return long(value)
        # convert to unicode
        elif c_type in ('UTF8Type', 'BytesType', 'NoneType'):
            return smart_str(value)
        # convert CompositeType
        if re.findall(r'^CompositeType\((.+)\)$',c_type):
            return tuple(map(self.convert_type,value, eval(c_type).components))
        else:
            raise ValueError,u"Unsupported type %s"%c_type
