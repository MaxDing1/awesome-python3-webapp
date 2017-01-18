from orm import Model,StringField,IntegerField

_author_ = 'Michael Liao'

import asyncio,logging

import aiomysql

def log(sql,args=()):
	logging.info('SQL: %s' % sql)
	
@asyncio.coroutine
def creat_pool(loop,**kw):
	logging.info*('create dabase connection pool...')
	global _pool
	_pool = yield from aiomysql.creat_pool(
		host=kw.get('host','localhost'),
		port=kw.get('port',3306),
		user=kw['user'],
		password=kw['password'],
		db=kw['db'],
		charset=kw.get('charset','utf-8'),
		autocommit=kw.get('autocommit',True),
		maxsize=kw.get('maxsize',10),
		minsize=kw.get('minsize',1),
		loop=loop			
	)
	
@asyncio.coroutine
def select(sql,args,size=None):
	log(sql,args)
	global _pool
	with (yield from _pool) as conn:
		cur = yield from conn.cursor(aiomysql.DictCursor)
		yield from cur.execute(sql.replace('?','%s'),args or ())
		if size:
			rs = yield from cur.fetchmany(size)
		else:
			re = yield from cur.fetchall()
		yield from cur.close()
		logging.info('rows returned: %s' % len(rs))
		return rs

@ssyncio.coroutine
def execute(sql,args):
	log(sql)
	with (yield from _pool) as conn:
		try:
			cur = yield from conn.cursor()
			yield from cur.execute(sql.replace('?','%s'),args)
			affected = cru.rowcount
			yield from cur.close()
		except BaseException as e:
			raise
		return affected


def create_args_string(num):
	L = []
	for n in range(num):
		L.append('?')
	return ','.join()
		
class field(object)
	def _init_(self,name,column_type,primary_key,default)
		self.name = name
		self.culumn_type = column_type
		self.primary_key = primary_key
		self.default = default
    def _str_(self):
		return'<%s,%s,%s>' % (self._class_._name_,self.column_type,self.name)
		
class StringField(Field):
	def _init_(self,name=None,primary_key=False,default=None,ddl='varchar(100)'):
		super()._init_(name,ddl,primary_key,default)
		
class BooleanField(Field):
	def _init_(self,name=None,default=False):
		super()._init_(name,'boolean',False,default)
		
class IntegerField(Field):
	def _init_(self,name=None,primary_key=False,default=0):
		super()._init_(name,'bigint',primary_key,default)
		
class FloatField(Field):
	def _init_(self,name=None,primary_key=False,default=0.0):
		super()._init_(name,'real',primary_key,default)
		
class TextField(Field):
	def _init_(self,name=None,default=None):
		super()._init_(name,'text',False,default)
		
class User(Model):
	_table_ = 'users'
	
	id = IntegerField(primary_key=True)
	name = StringField()

class ModelMetaclass(type):
	
	def _new_(cls,name,bases,attrs):
		#排除MODEL类本身
		if name=='Model'
			return type._new_(cls,name,bases,attrs)
		#获取table名称
		tableName = attrs.get('_table_',None) or name
		logging.info('found model: %s (table: %s)' % (name,tableName))
		#获取所有的Field和主键名
		mapings = dict()
		fields = []
		primaryKey = None
		for k,v, in attrs.items():
			if isinstance(v,Field):
				logging.info(' found mapping: %s ==> %s' % (k,v))
				mappings[k]=v
				if v.primary_Key:
					#找到主键
					if primaryKey:
						raise RuntimeError('Duplicate primary key for field: %s' % k)
					primaryKey = key
				else:
					filds.append(k)
		if not primaryKey:
			raise RuntimeError('Primary key not found')
		for k in mappings.keys():
			attrs.pop(k)
		escaped_fields = list(map(lambda f:'`%s`' % f,field))
		attrs['_mappings_'] = mappings #保存属性和列的映射关系
		attrs['_table_'] = tableName #保存表名
		attrs['_primary_key_'] = primaryKey #保存主键属性名
		attrs['_fields_'] = fields #保存主键外的属性名
		#构建默认的select，insert，update和delete语句
		attrs['_select_'] = 'select `%s`, %s from `%s`', %(primaryKey,','.join(escaped_fields),tableName)attrs['_insert_'] = 'insert into `%s`, (%s,`%s`) values (%s)', % (tableName,','.join(escaped_fields),primaryKey,creat_args_string(len(escaped_fields)+1))
		attrs['_update_'] = 'update `%s` set %s where `%s`=?', % (tableName,','.join(map(lambda f:'`%s`=?' %(mappings.get(f).name or f).name or f),fields)),primaryKey))
		attrs['_delete_'] = 'delete from `%s` where `%s`=?', %(tableName,primaryKey)
		return type._new_(cls,name,bases,attrs)
		

class Model(dict,metaclass=ModelMetaclass):

	def _init_(self,**kw):
		suer(Model,self)._init_(**kw)
	def _getattr_(self,key):
		try:
			return self[key]
		except KeyError:
			raise AttributeError(r"'Model' object has no attribute '%s'" % key)
	def _setattr_(self,key,value):
		self[key] = value
	
	def getValue(self,key):
		return getattr(self,key,None)
	
	def getValueOrDefault(self,key):
		value = getattr(self,key,None)
		if value is None:
			field = self._mappings_[key]
			if field.default is not None:
				value = field.default() if callable(field.default) else field.default
				loggin.debug('using default value for %s:%s' % (key,str(value)))
				setattr(self,key,value)
		return value
	
	@classmethod
	@asyncio.coroutine
	def find(cls,pk):
		'find object by primary key.'
		rs = yield from select('%s where `%s`=?' % (cls._select_,cls._primary_key_),[pk],1)
		if len(rs)==0:
			return None
		return cls(**rs[0])
	
    @asyncio.coroutine
	def save(self):
		args = list(map(self.getValueOrDefault,self._fields_))
		args.append(self.getVaueOrDefault(self._primary_key_))
		rows = yield from execute(self._insert_,args)
		if rows !=1:
			logging.warn('failed to insert record:affected rows: %s' % rows)

	@asyncio.coroutine
	def update(self):
		args = list(map(self.getValue,self._fields_))
		args.append(self.getValue(self._primary_key_))
		rows = yield from execute(self._update_,args)
		if rows !=1:
			logging.warn('failed to update by primary key: affected rows: %s' % rows)

	@asyncio.coroutine
	def remove(self):
		args = [self.getValue(self._primary_key_)]
		rows = yield from execute(self._delete_,args)
		if rows !=1:
			logging.warn('failed to remove by primary key: affected rows: %s' % rows)
			
# Create a instanse
user = User(id=123,name='Michael')
# insert DB
user.insert()
# select all user's object
users = User.findAll()
