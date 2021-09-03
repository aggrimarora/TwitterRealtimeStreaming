import matplotlib.pyplot as plt
from matplotlib import animation
from matplotlib import style
import sys

style.use('ggplot')
fig = plt.figure()
ax = fig.add_subplot(1,1,1)


def animate(interval):
    # fetches data every 2 seconds
	graph_data = open(sys.argv[1], 'r').read()
	lines = graph_data.split('\n')
	xs = []
	ys = []
	for line in lines:
		if len(line) > 1:
			# update the count of existing data after reading new data from file
			x, y = line.split()
			xs.append(x)
			ys.append(int(y))
	ax.clear()
	ax.barh(xs, ys)
	ax.set_xlabel('number of #', fontsize=12)
	ax.plot()

# updates the plot dynamically based on the data received in the file
anime = animation.FuncAnimation(fig, animate, interval=2000)
plt.show()
