import yaml


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
	_m_to_m_create = 		    	
			
	def __init__(self, yaml_file):
		with open(yaml_file, 'r') as file:
			self.schema = yaml.load(file)
	
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
			


if __name__ == '__main__':

	a = SqlYamler('./tables.yaml')
	a.query()

