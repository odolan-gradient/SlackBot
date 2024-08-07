import matplotlib.pyplot as plt
import numpy as np


xpoints = np.array([37, 39, 123, 92, 97, 74, 133, 108, 36, 215, 195, 183, 50, 59, 33, 40, 46, 73, 83, 56, 136, 54, 91, 74])
ypoints = np.array([93, 92, 87, 93, 91, 91, 82, 78, 95, 77, 78, 75, 94, 89, 96, 95, 95, 94, 92, 91, 85, 91, 86, 91])

# Sort the points based on x-values
sorted_indices = np.argsort(xpoints)
xpoints_sorted = xpoints[sorted_indices]
ypoints_sorted = ypoints[sorted_indices]

plt.plot(xpoints_sorted, ypoints_sorted)
plt.show()
