import pandas as pd
import matplotlib.pyplot as plt

data = pd.read_csv("./greatest_disaster_by_state/part-unified_disaster_by_state.csv")

data = data.sort_values(by=['UNIFIED_DEMAGE'],ascending=False)
print data.head()

plt.ylabel('Unified demage by state')
plt.xticks(rotation=90)
plt.bar(data['STATE'], data['UNIFIED_DEMAGE'])

# plt.show()
figure = plt.gcf() # get current figure
figure.set_size_inches(13, 6)
plt.savefig('bar_plot_unified_disaster_by_state.png',bbox_inches='tight',dpi=600)