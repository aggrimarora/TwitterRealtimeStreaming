import matplotlib.pyplot as plt
from matplotlib import animation
from matplotlib import style
import sys

#position subplots in the figure
fig = plt.figure()
ax = [None, None, None, None, None]
ax[0] = fig.add_subplot(331)
ax[1] = fig.add_subplot(333)
ax[2] = fig.add_subplot(335)
ax[3] = fig.add_subplot(337)
ax[4] = fig.add_subplot(339)

plot_labels = ['Positive', 'Negative', 'Neutral']
topic_real_time_data = {}

# check whether a post is positive, negative or neutral
def get_sentiment(s):
    if s == "neu":
        return 2
    elif s == "neg":
        return 1
    return 0

def animate(interval):
    # fetches data every 2 seconds
    graph_data = open(sys.argv[1], 'r').read()
    lines = graph_data.split('\n')
    for line in lines:
        if len(line) > 1:
            x, y = line.split()
            topic, sentiment = x.split(':')
            index = get_sentiment(sentiment)
            #if topic does not exist, initialize with default values
            if topic not in topic_real_time_data.keys():
                topic_real_time_data[topic] = {
                        "data": [0,0,0],
                        "count": 0
                    }
            
            topic_real_time_data[topic]["data"][index] = int(y)
            topic_real_time_data[topic]["count"] = sum(topic_real_time_data[topic]["data"])

            topics = topic_real_time_data.keys()
            topics.sort()
            i = 0
            #update the pie charts for each topic after reading new data from the file
            for t in topics:
                ax[i].clear()
                ax[i].pie(topic_real_time_data[t]["data"], labels = plot_labels, colors = ['green', 'red', 'yellow'])
                ax[i].set_title(t)
                ax[i].plot()
                i += 1
            # plt.show()

            print(topic_real_time_data)

anime1 = animation.FuncAnimation(fig, animate,interval=2000)
plt.show()

