import pandas as pd
import matplotlib.pyplot as plt


data = pd.read_csv("./greatest_disaster_by_state/part-disaster_by_state.csv")


data = data.sort_values(by=['INJURIES'],ascending=False)
print data.head()

plt.ylabel('Injuris by state')
plt.xticks(rotation=90)
plt.bar(data['STATE'], data['INJURIES'])

# plt.show()
figure = plt.gcf() # get current figure
figure.set_size_inches(13, 6)
plt.savefig('injuris_by_state.png',bbox_inches='tight',dpi=600)

#########################################################
data = data.sort_values(by=['DEATHS'],ascending=False)
print "#######################"
print data.head()

plt.ylabel('Deaths by state')
plt.xticks(rotation=90)
plt.bar(data['STATE'], data['DEATHS'])

# plt.show()
figure = plt.gcf() # get current figure
figure.set_size_inches(13, 6)
plt.savefig('deaths_by_state.png',bbox_inches='tight',dpi=600)

#########################################################
data = data.sort_values(by=['MATERIAL_DAMAGE'],ascending=False)
print "#######################"
print data.head()

plt.ylabel('Material damage by state')
plt.xticks(rotation=90)
plt.bar(data['STATE'], data['MATERIAL_DAMAGE'])

# plt.show()
figure = plt.gcf() # get current figure
figure.set_size_inches(13, 6)
plt.savefig('material_damage_by_state.png',bbox_inches='tight',dpi=600)