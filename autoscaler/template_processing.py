from copy import deepcopy
import json

def delete_nsg(template):
    nsg_resource_index = -1
    template = deepcopy(template)
    resources = template['resources']
    for i in range(len(resources)):
        resource_type = resources[i]['type']
        if resource_type == 'Microsoft.Network/networkSecurityGroups':
            nsg_resource_index = i
        if resource_type == 'Microsoft.Network/virtualNetworks':
            dependencies = resources[i]['dependsOn']
            for j in range(len(dependencies)):
                # Delete any dependency on the NSG
                if dependencies[j] == "[concat('Microsoft.Network/networkSecurityGroups/', variables('nsgName'))]":
                    dependencies.pop(j)
                    break
    resources.pop(nsg_resource_index)
    return template

def unroll_vm(template, pool, new_pool_size):
    """
    unroll_vm transform an ARM template by replacing the VirtualMachine resource (for the specified pool) that has a Count function
    by multiple singular ones
    """
    resources = template['resources']
    vm_resource_name = "[concat(variables('{}VMNamePrefix'), copyIndex(variables('{}Offset')))]".format(pool.name, pool.name)
    vm_template = None
    
    for i in range(len(resources)):
        if resources[i]['name'] == vm_resource_name:
            vm_template = deepcopy(resources[i])
            resources.pop(i)
            break

    if not vm_template:
        raise ValueError('Could not find the virtualMachines resource for the specified agent pool')
    
    new_idxs = get_new_nodes_indexes(pool, new_pool_size)
    for index in new_idxs:  # replace by new node indexes
        node_vm_template = deepcopy(vm_template)
        #remove the copy function
        node_vm_template.pop('copy')
        node_vm_template['name'] = "[concat(variables('{}VMNamePrefix'), {})]".format(
            pool.name, index)
        
        #replace all occurence of copyIndex(variables('<pool_name>Offset')) by the actual index
        str_template = json.dumps(node_vm_template)
        str_template = str_template.replace("copyIndex(variables('{}Offset'))".format(pool.name), str(index))
        node_vm_template = json.loads(str_template)
        resources.insert(0, node_vm_template)
        
    return template

def unroll_vm_extension(template, pool, new_pool_size):
    """
    unroll_vm_extension transform an ARM template by replacing the virtualMachines/extensions resource (for the specified pool) that has a Count function
    by multiple singular ones
    """
    resources = template['resources']
    ext_resource_name = "[concat(variables('{}VMNamePrefix'), copyIndex(variables('{}Offset')),'/cse', copyIndex(variables('{}Offset')))]".format(pool.name, pool.name, pool.name)
    ext_template = None
    
    for i in range(len(resources)):
        if resources[i]['name'] == ext_resource_name:
            ext_template = deepcopy(resources[i])
            resources.pop(i)
            break

    if not ext_template:
        raise ValueError('Could not find the virtualMachines/extensions resource for the specified agent pool')
    
    new_idxs = get_new_nodes_indexes(pool, new_pool_size)
    for index in new_idxs:  # replace by new node indexes
        node_ext_template = deepcopy(ext_template)
        #remove the copy function
        node_ext_template.pop('copy')
        node_ext_template['name'] = "[concat(variables('{}VMNamePrefix'), {},'/cse', {})]".format(pool.name, index, index)
        
        #replace all occurence of copyIndex(variables('<pool_name>Offset')) by the actual index
        str_template = json.dumps(node_ext_template)
        str_template = str_template.replace("copyIndex(variables('{}Offset'))".format(pool.name), str(index))
        node_ext_template = json.loads(str_template)
        resources.insert(0, node_ext_template)
        
    return template

def unroll_nic(template, pool, new_pool_size):
    """
    unroll_nic transform an ARM template by replacing the NetworkInterface resource (for the specified pool) that has a Count function
    by multiple singular ones
    """
    resources = template['resources']
    nic_prefix = "[concat(variables('{}VMNamePrefix'), 'nic-'".format(pool.name)
    nic_template = None
    
    for i in range(len(resources)):
        if resources[i]['name'].startswith(nic_prefix):
            nic_template = deepcopy(resources[i])
            resources.pop(i)
            break

    if not nic_template:
        raise ValueError('Could not find the NIC resource for the specified agent pool')
    
    new_idxs = get_new_nodes_indexes(pool, new_pool_size)
    for index in new_idxs:  # replace by new node indexes
        node_nic_template = deepcopy(nic_template)
        #remove the copy function
        node_nic_template.pop('copy')
        node_nic_template['name'] = "[concat(variables('{}VMNamePrefix'), 'nic-', {})]".format(
            pool.name, index)
        resources.insert(0, node_nic_template)
        
    return template


def unroll_resources(template, pools, new_pool_sizes):
    """"
    unroll NICs, VMs, and VM extensions resources into multiple
    resources instead of using count().
    Storage accounts can still use Count, so no modification needed.
    """
    template = deepcopy(template)
    for pool in pools:
        new_pool_size = new_pool_sizes[pool.name]
        if pool.actual_capacity == new_pool_size:
            #When the pool doesn't changes size, we don't need to make any modifictions
            continue

        template = unroll_nic(template, pool, new_pool_size)
        template = unroll_vm(template, pool, new_pool_size)
        template = unroll_vm_extension(template, pool, new_pool_size)
    return template


def get_new_nodes_indexes(pool, new_pool_size):
    """
    get_new_nodes_indexes returns the index of the new nodes that would be created
    by scaling the specified pool to the specified size.
    For example, if pool currently has 2 agents with index 2 and 4
    With a new_pool_sizes of 5, the new indexes would be 0, 1 and 3
    """
    indexes = []
    i = 0
    idx = 0
    while i < new_pool_size - pool.actual_capacity:
        if pool.has_node_with_index(idx):
            idx += 1
            continue
        indexes.append(idx)
        i += 1
        idx += 1
    return indexes
