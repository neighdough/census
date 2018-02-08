'''
Created on Jan 28, 2014

@author: nfergusn
'''

import os
import zipfile as zip
from census import shp_2_pgsql

census_folder = 'S:\DATA\Census\TIGER\geo'
outdir = 'S:\DATA\Census\TIGER\SHP'
subdirs = []
print 'Extracting zip files... '
# for root, dirs, files in os.walk(census_folder):    
#     for d in dirs:
#         if not os.path.exists(os.path.join(outdir, d)):
#             os.mkdir(os.path.join(outdir, d))
#             subdirs.append(d)
#  
#  
#     for f in files:
#         if f.endswith('.zip'):
# #             print root, f
#             z = zip.ZipFile(os.path.join(root, f))
#             z.extractall(path=os.path.join(outdir, root.split('\\')[-1]))
# #              
for root, dirs, files in os.walk(outdir):
    if len(dirs) > 0:
        for d in dirs:
            subdirs.append(d)
    else:
        break
#     for d in dirs:
#         if not d is None:
#             print d
#             subdirs.append(d)

for subdir in subdirs:
    print '\tLoading ', subdir
    shp_2_pgsql.add_shp(os.path.join(outdir, subdir), subdir)
