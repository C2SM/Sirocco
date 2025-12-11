"""Analyze branch-independence test timing data."""

from aiida import load_profile, orm
from aiida_workgraph.orm.workgraph import WorkGraphNode
import pandas as pd
import matplotlib
matplotlib.use('Agg')  # Non-interactive backend for HPC
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches

# Load AiiDA profile
load_profile()

# Get the WorkGraph PK from command line or hardcode it
import sys
if len(sys.argv) > 1:
    pk = int(sys.argv[1])
else:
    pk = 9019  # Default PK

print(f"Analyzing WorkGraph PK: {pk}")

# Load main WorkGraph
n = orm.load_node(pk)

# Extract launcher WorkGraph nodes with both creation and completion times
timing_data = []
for desc in n.called_descendants:
    if isinstance(desc, WorkGraphNode) and desc.label.startswith('launch_'):
        # Parse task name from label: "launch_fast_1_date_..." → "fast_1"
        label_parts = desc.label.split('_')
        if len(label_parts) >= 3:
            # Handle both "launch_root_date_..." and "launch_fast_1_date_..."
            if label_parts[1] in ['root', 'fast', 'slow']:
                if label_parts[1] == 'root':
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
    print("ERROR: No launcher tasks found!")
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
print("\n" + "="*80)
print("TASK SUBMISSION AND COMPLETION TIMELINE")
print("="*80)
print(df[['task', 'branch', 'start_rel', 'end_rel', 'duration']].to_string(index=False))

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
ax.set_title('Branch Independence: Launcher Creation and Completion Timeline\n' +
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
print(f"\nGantt chart saved to: {output_file}")

# Validation: Check if fast branch completed before slow branch
print("\n" + "="*80)
print("DYNAMIC LEVEL VALIDATION")
print("="*80)

fast_tasks = df[df['branch'] == 'fast'].sort_values('end_rel')
slow_tasks = df[df['branch'] == 'slow'].sort_values('end_rel')

if not fast_tasks.empty and not slow_tasks.empty:
    last_fast = fast_tasks.iloc[-1]
    last_slow = slow_tasks.iloc[-1]

    print(f"Fast branch last task: {last_fast['task']} completed at {last_fast['end_rel']:.1f}s")
    print(f"Slow branch last task: {last_slow['task']} completed at {last_slow['end_rel']:.1f}s")

    if last_fast['end_rel'] < last_slow['end_rel']:
        print("✓ PASS: Fast branch completed before slow branch")
    else:
        print("✗ FAIL: Fast branch did NOT complete before slow branch")
        print("       This suggests a problem with dynamic level scheduling!")

    # Check submission order (when launchers were created)
    print("\n" + "-"*80)
    print("SUBMISSION ORDER ANALYSIS")
    print("-"*80)

    fast_3 = df[df['task'] == 'fast_3']
    slow_3 = df[df['task'] == 'slow_3']

    if not fast_3.empty and not slow_3.empty:
        fast_3_start = fast_3.iloc[0]['start_rel']
        slow_3_start = slow_3.iloc[0]['start_rel']

        print(f"fast_3 launcher created at: {fast_3_start:.1f}s")
        print(f"slow_3 launcher created at: {slow_3_start:.1f}s")
        print(f"Difference: {abs(fast_3_start - slow_3_start):.1f}s")

        if fast_3_start < slow_3_start:
            print("✓ fast_3 was submitted BEFORE slow_3 (correct)")
        else:
            print("✗ fast_3 was submitted AFTER slow_3 (INCORRECT!)")
            print("  This indicates a BUG - fast branch should be prioritized")

print("\n" + "="*80)
print("ANALYSIS COMPLETE")
print("="*80)
