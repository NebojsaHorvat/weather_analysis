import pandas as pd
import matplotlib.pyplot as plt


data = pd.read_csv("./demage_by_disaster_type_hadoop.csv")


data = data.sort_values(by=['INJURIES'],ascending=False)
data_above = data[:10]
data_below = data[10:]
data_below_sum = data_below.sum()
df2 = pd.DataFrame([ ["Other", data_below_sum['INJURIES'], data_below_sum['DEATHS'], data_below_sum['MATERIAL_DAMAGE']]], 
    columns=['DISASTER_TYPE','INJURIES','DEATHS','MATERIAL_DAMAGE'])

data_above = data_above.append(df2,ignore_index=True)
print "##################"
print data_above.head(12)
plt.ylabel('Injuris by disaster type')

porcent = 100.*data_above['INJURIES']/data_above['INJURIES'].sum()
colors = ['yellowgreen','red','gold','lightskyblue','magenta','lightcoral','blue','pink', 'darkgreen','yellow','grey']#,'violet','cyan'
labels = ['{0} - {1:1.2f} %'.format(i,j) for i,j in zip(data_above['DISASTER_TYPE'], porcent)]
patches, texts = plt.pie(data_above['INJURIES'],colors = colors, startangle=90)
plt.legend(patches, labels, loc="best")
# Set aspect ratio to be equal so that pie is drawn as a circle.
plt.axis('equal')
plt.tight_layout()
figure = plt.gcf() # get current figure
figure.set_size_inches(13, 6)
plt.savefig('injuris_by_disaster_type.png',bbox_inches='tight',dpi=600)


# #########################################################
data = data.sort_values(by=['DEATHS'],ascending=False)
data_above = data[:10]
data_below = data[10:]
data_below_sum = data_below.sum()
df2 = pd.DataFrame([ ["Other", data_below_sum['INJURIES'], data_below_sum['DEATHS'], data_below_sum['MATERIAL_DAMAGE']]], 
    columns=['DISASTER_TYPE','INJURIES','DEATHS','MATERIAL_DAMAGE'])

data_above = data_above.append(df2,ignore_index=True)
print "##################"
print data_above.head(12)
plt.ylabel('Mateial damage by disaster type')

porcent = 100.*data_above['DEATHS']/data_above['DEATHS'].sum()
colors = ['yellowgreen','red','gold','lightskyblue','magenta','lightcoral','blue','pink', 'darkgreen','yellow','grey']#,'violet','cyan'
labels = ['{0} - {1:1.2f} %'.format(i,j) for i,j in zip(data_above['DISASTER_TYPE'], porcent)]
patches, texts = plt.pie(data_above['DEATHS'],colors = colors, startangle=90)
plt.legend(patches, labels, loc="best")
# Set aspect ratio to be equal so that pie is drawn as a circle.
plt.axis('equal')
figure = plt.gcf() # get current figure
figure.set_size_inches(13, 6)
plt.savefig('deaths_by_disaster_type.png',bbox_inches='tight',dpi=600)

# #########################################################
data = data.sort_values(by=['MATERIAL_DAMAGE'],ascending=False)
data_above = data[:10]
data_below = data[10:]
data_below_sum = data_below.sum()
df2 = pd.DataFrame([ ["Other", data_below_sum['INJURIES'], data_below_sum['DEATHS'], data_below_sum['MATERIAL_DAMAGE']]], 
    columns=['DISASTER_TYPE','INJURIES','DEATHS','MATERIAL_DAMAGE'])

data_above = data_above.append(df2,ignore_index=True)
print "##################"
print data_above.head(12)
plt.ylabel('Mateial damage by disaster type')

porcent = 100.*data_above['MATERIAL_DAMAGE']/data_above['MATERIAL_DAMAGE'].sum()
colors = ['yellowgreen','red','gold','lightskyblue','magenta','lightcoral','blue','pink', 'darkgreen','yellow','grey']#,'violet','cyan'
labels = ['{0} - {1:1.2f} %'.format(i,j) for i,j in zip(data_above['DISASTER_TYPE'], porcent)]
patches, texts = plt.pie(data_above['MATERIAL_DAMAGE'],colors = colors, startangle=90)
plt.legend(patches, labels, loc="best")
# Set aspect ratio to be equal so that pie is drawn as a circle.
plt.axis('equal')
figure = plt.gcf() # get current figure
figure.set_size_inches(13, 6)
plt.savefig('material_damage_by_disaster_type.png',bbox_inches='tight',dpi=600)