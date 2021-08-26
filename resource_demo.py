# -*- coding: utf-8 -*-

import pandas as pd
import resource_pb2

resource_message = resource_pb2.Resource()


df = pd.read_csv("resources.csv")


new_df = df[['link', 'dataset', 'table', 'env', 'platform', 'field_name1', 'dimension1', 'field_name2', 
'dimension2', 'field_name3', 'dimension3', 'field_name4', 'dimension4', 'field_name5', 'dimension5', 
'field_name6', 'dimension6', 'field_name7', 'dimension7', 'field_name8', 'dimension8', 'field_name9',
'dimension9', 'field_name10', 'dimension10', 'fa_hierarchy', 'scop']]

print(new_df)

def check_nan(data):
	return "" if data != 'nan' else data


counter = 1

for item in new_df.values:
	item = list(item)
	resource_message.id = counter
	resource_message.entry.add(
		link=item[0],
		dataset=item[1], 
		table=item[2], 
		env=item[3], 
		platform=item[4],
		field_name1=item[5],
		dimension1=item[6],
		field_name2=check_nan(item[7]),
		dimension2=check_nan(item[8]),
		field_name3=check_nan(item[9]),
		dimension3=check_nan(item[10]),
		field_name4=check_nan(item[11]),
		dimension4=check_nan(item[12]),
		field_name5=check_nan(item[13]),
		dimension5=check_nan(item[14]),
		field_name6=check_nan(item[15]),
		dimension6=check_nan(item[16]),
		field_name7=check_nan(item[17]),
		dimension7=check_nan(item[18]),
		field_name8=check_nan(item[19]),
		dimension8=check_nan(item[20]),
		field_name9=check_nan(item[21]),
		dimension9=check_nan(item[22]),
		field_name10=check_nan(item[23]),
		dimension10=check_nan(item[24]),
		fa_hierarchy=item[25],
		scop=item[26]
	)
	counter = counter + 1


print(resource_message)


with open("resources.bin", "wb") as f:
    print("write as binary")
    bytesAsString = resource_message.SerializeToString()
    f.write(bytesAsString)


with open("resources.bin", "rb") as f:
    print("read values")
    resource_read = resource_pb2.Resource().FromString(f.read())


print(resource_read)


