pip install numpy pandas pyarrow

## Exemple python

# import des libs
#!/usr/bin/python3
import pyarrow as pa
import pandas as pd
import numpy as np
import pyarrow.parquet as pq

# création du dataframe
df = pd.DataFrame({'col1': [1.0,2.1,3.2], 'col2': ['paul','pierre','jean'],'col3': ['paris','marseille','rouen']})

# conversion en pyarrow
table = pa.Table.from_pandas(df)

# écriture du fichier en parquet
pq.write_table(table, 'example.parquet')

# check chargement fichier et lecture
table2 = pq.read_table('example.parquet')
table2.to_pandas()


# Site officiel Apache Parquet 
https://parquet.apache.org/

wget https://github.com/Teradata/kylo/raw/master/samples/sample-data/parquet/userdata1.parquet

pip install parquet-cli

parq userdata1.parquet
parq userdata1.parquet --head 3
parq userdata1.parquet --tail 3
parq userdata1.parquet --count
