import yaml
class NotSupported(Exception): pass

class SqlYamler(object):
	_create_query = '''CREATE TABLE {table}(
			{table}_ID INT PRIMARY KEY, 
			{fields},
			{table}_created timestamp DEFAULT now(), 
			{table}_updated timestamp DEFAULT now()
			)\n
					'''
					
	_trigger_query = '''\rCREATE OR REPLACE FUNCTION
			update_{table}() RETURNS TRIGGER AS $$
                BEGIN
                    NEW.{table}_updated = now();
				            RETURN NEW;
			       END;
			   $$ LANGUAGE plpgsql;			    	
			\rCREATE TRIGGER {table}_upd_trigger AFTER UPDATE ON {table}
			FOR EACH ROW EXECUTE PROCEDURE update_{table}();\n\n
			'''
	_one_to_many = '''
			\rALTER TABLE {child} ADD {parent}_ID INT NOT NULL,
			ADD CONSTRAINT FK_{child}{parent}
			FOREIGN KEY {parent}_ID REFERENCES {parent}({parent}_ID);
			'''
	_linking_table = '''
			\rCREATE TABLE {table1}_{table2}(
			{table1}_ID INT NOT NULL,
			{table2}_ID INT NOT NULL,
			PRIMARY KEY ({table1}_ID, {table2}_ID)
			);
			'''
	_many_to_many = '''
			ALTER TABLE {table1}_{table2},
				ADD CONSTRAINT FK_{table1}_{table2}__{table1}_ID
				FOREIGN KEY {table1}_ID REFERENCES {table1}({table1}_ID);
			ALTER TABLE {table1}_{table2},
				ADD CONSTRAINT FK_{table1}_{table2}__{table2}_ID
				FOREIGN KEY {table2}_ID REFERENCES {table2}({table2}_ID); 
			'''

	def __init__(self, yaml_file):
		with open(yaml_file, 'r') as file:
			self.schema = yaml.load(file)
			self.many_to_many = dict()
			self.one_to_many = dict()
			self.many_to_one = dict()
			self.__get_relations()
	
	def _fields(self, entity):
		fields_list = []
		for field in self.schema[entity].values():
			for key in field:
				fields_list.append(str(key)+ ' ' + str(field[key]))
		return ', '.join(fields_list)

	def query(self):
		with open("./yml_schema.sql",'w') as output:
			for entity in self.schema.keys():
				output.write(self._create_query.format(table=entity, fields = self._fields(entity)))
				output.write(self._trigger_query.format(table=entity))
			output.write(self._relation_query())
	
	def __get_relations(self):
		for entity in self.schema.keys():
			if 'relations' in self.schema[entity]:
				keens = self.schema[entity]['relations']
				for keen in keens:
					if keens[keen] == 'one':
						if self.schema[keen]['relations'][entity] == 'many':
							self.one_to_many[(keen, entity)] = 1
							
					if keens[keen] == 'many':
						if self.schema[keen]['relations'][entity] == 'many':
							if (entity, keen) not in self.many_to_many:
								self.many_to_many[(keen, entity)] = 1
							else:
								self.many_to_many[(entity, keen)] += 1
						if self.schema[keen]['relations'][entity] == 'one':
							self.many_to_one[(keen, entity)] = 1
		
	def _relation_query(self):	
		
		relation_query  = ''
		for relation in self.many_to_many:
			if self.many_to_many[relation] != 2:
				raise NotSupported
			relation_query =''.join((
				__class__._linking_table.format(
								table1=relation[0],
								table2=relation[1]
								),
				 
				 __class__._many_to_many.format(
								table1=relation[0],
								table2=relation[1]
								)
				))						
	
		for relation in self.one_to_many:
			if self.many_to_one[relation[::-1]] != 1:
				raise NotSupported
			relation_query = ''.join( (relation_query,
				 __class__._one_to_many.format(
								child=relation[1],
								parent=relation[0]
								)
				))
		return relation_query
			
	


if __name__ == '__main__':

	a = SqlYamler('./tables.yaml')
	a.query()

