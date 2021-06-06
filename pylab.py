import json

#text: {'a,b':'20';'c,d':'30'}
def dict(text):
	keys_and_values = ""
	subjects = text.split(';')
	for each_sub in subjects:
		# remove whitespace
		each_sub = (each_sub.rstrip()).lstrip()
		keys = each_sub.split(':')[0]
		# remove whitespace
		keys = (keys.rstrip()).lstrip()
		keys_value = each_sub.split(':')[1]

		# remove whitespace
		keys_value = (keys_value.rstrip()).lstrip()
		# converts string to json format
		if keys_value == "True":
			keys_value = "true"
		elif keys_value == "False":
			keys_value = "false"

		if ", " in keys:
			for index, key in enumerate(keys.split(', ')):
				if index == 0:
					if len(keys_and_values) == 0:
						keys_and_values = f'"{key}":{keys_value}'
					else:
						keys_and_values += f', "{key}":{keys_value}'
				else:
					keys_and_values += f', "{key}":{keys_value}'
		elif "," in keys:
			for index, key in enumerate(keys.split(',')):
				if index == 0:
					if len(keys_and_values) == 0:
						keys_and_values = f'"{key}":{keys_value}'
					else:
						keys_and_values += f', "{key}":{keys_value}'
				else:
					keys_and_values += f', "{key}":{keys_value}'
	string_dict = '{' + keys_and_values + '}'
	# converts string to dict # Cannot convert boolean of string to dict
	dictionary = json.loads(string_dict)
	return dictionary