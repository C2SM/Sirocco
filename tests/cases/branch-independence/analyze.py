"""Analyze branch-independence test timing data."""

import matplotlib as mpl
import pandas as pd
from aiida import load_profile, orm
from aiida_workgraph.orm.workgraph import WorkGraphNode

mpl.use('Agg')  # Non-interactive backend for HPC
import matplotlib.patches as mpatches
import matplotlib.pyplot as plt

# Load AiiDA profile
load_profile()

# Get the WorkGraph PK from command line or hardcode it
import sys

if len(sys.argv) > 1:
    pk = int(sys.argv[1])
else:
    pk = 9019  # Default PK


# Load main WorkGraph
n = orm.load_node(pk)

# Extract launcher WorkGraph nodes with both creation and completion times
timing_data = []
for desc in n.called_descendants:
    # breakpoint()
    if isinstance(desc, WorkGraphNode) and desc.label.startswith('launch_'):
        # Parse task name from label: "launch_fast_1_date_..." → "fast_1"
        label_parts = desc.label.split('_')
        if len(label_parts) >= 3:
            # Handle both "launch_root_date_..." and "launch_fast_1_date_..."
            if label_parts[8] in ['root', 'fast', 'slow']:
                if label_parts[8] == 'root':
                    task_name = 'root'
                    branch = 'root'
                else:
                    task_name = f"{label_parts[1]}_{label_parts[2]}"
                    branch = label_parts[1]

                timing_data.append({
                    'task': task_name,
                    'label': desc.label,
                    'start': desc.ctime,  # When launcher was created
                    'end': desc.mtime,    # When launcher finished
                    'branch': branch,
                    'pk': desc.pk
                })

if not timing_data:
    sys.exit(1)

# Create DataFrame and sort by start time
df = pd.DataFrame(timing_data)
df = df.sort_values('start')

# Calculate relative times (seconds from workflow start)
workflow_start = df['start'].min()
df['start_rel'] = (df['start'] - workflow_start).dt.total_seconds()
df['end_rel'] = (df['end'] - workflow_start).dt.total_seconds()
df['duration'] = df['end_rel'] - df['start_rel']

# Print summary table

# Create Gantt chart
fig, ax = plt.subplots(figsize=(14, 8))

# Define colors
colors = {'fast': '#2E86AB', 'slow': '#A23B72', 'root': '#333333'}

# Sort tasks for better visualization (root first, then by branch and number)
def sort_key(task):
    if task == 'root':
        return (0, 0)
    parts = task.split('_')
    if len(parts) == 2:
        branch_order = {'fast': 1, 'slow': 2}.get(parts[0], 3)
        try:
            num = int(parts[1])
        except:
            num = 0
        return (branch_order, num)
    return (3, 0)

sorted_tasks = sorted(df['task'], key=sort_key)
y_positions = {task: i for i, task in enumerate(sorted_tasks)}

# Plot horizontal bars for each launcher
for _, row in df.iterrows():
    y_pos = y_positions[row['task']]
    color = colors.get(row['branch'], 'gray')

    ax.barh(y_pos, row['duration'], left=row['start_rel'],
            height=0.6, color=color, alpha=0.8, edgecolor='black', linewidth=0.5)

    # Add duration label at the end of the bar
    ax.text(row['end_rel'] + 0.5, y_pos, f"{row['duration']:.1f}s",
            va='center', fontsize=9)

# Customize plot
ax.set_yticks(range(len(sorted_tasks)))
ax.set_yticklabels(sorted_tasks)
ax.set_xlabel('Time (seconds from workflow start)', fontsize=12)
ax.set_ylabel('Task', fontsize=12)
ax.set_title('Branch Independence: Launcher Creation and Completion Timeline\n'
             '(Shows when WorkGraph engine submitted each task)',
             fontsize=14, fontweight='bold')
ax.grid(True, axis='x', alpha=0.3, linestyle='--')

# Add legend
legend_patches = [mpatches.Patch(color=colors[b], label=b.capitalize())
                  for b in ['root', 'fast', 'slow']]
ax.legend(handles=legend_patches, loc='lower right', fontsize=10)

# Add vertical line at workflow start
ax.axvline(x=0, color='green', linestyle='--', linewidth=2, alpha=0.7)

plt.tight_layout()
output_file = f'launcher_timeline_pk{pk}.png'
plt.savefig(output_file, dpi=200, bbox_inches='tight')

# Validation: Check if fast branch completed before slow branch

fast_tasks = df[df['branch'] == 'fast'].sort_values('end_rel')
slow_tasks = df[df['branch'] == 'slow'].sort_values('end_rel')

if not fast_tasks.empty and not slow_tasks.empty:
    last_fast = fast_tasks.iloc[-1]
    last_slow = slow_tasks.iloc[-1]


    if last_fast['end_rel'] < last_slow['end_rel']:
        pass
    else:
        pass

    # Check submission order (when launchers were created)

    fast_3 = df[df['task'] == 'fast_3']
    slow_3 = df[df['task'] == 'slow_3']

    if not fast_3.empty and not slow_3.empty:
        fast_3_start = fast_3.iloc[0]['start_rel']
        slow_3_start = slow_3.iloc[0]['start_rel']


        if fast_3_start < slow_3_start:
            pass
        else:
            pass

