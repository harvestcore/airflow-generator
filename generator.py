"""
    This function generates the individual (or parallel) operators
    to be used in a flow.
    
    It returns the following dictionary, with information
    about the single operator that has been computed.
    
        {
            'operator': Operator[] | Operator,
            'operator_id': str,
            'entities': {}<string, Operator>
        }
    
    Params:
    - dag: The Dag object to be used.
    - operator: The operator to be computed. This operator object has
    the following structure:
        {
            'id': str,          # Operator identifier.
            'type': Operator,   # Operator to be used.
            'params': {}        # Operator specific parameters.
        }
"""
def generate_operator(dag, operator):
    # This operator is parallel.
    if 'parallel' in operator:        
        # Output object.
        output = {
            # List of parallel operators.
            'operator': [],
            # Operator ID, so it can be referenced later.
            'operator_id': operator['id'],
            # Dict containing all the different operators used in this parallel.
            'entities': {}
        }
        
        # Compute all the operators in the parallel.
        for op in operator['parallel']:
            # Compute the operator.
            computed = generate_operator(dag, op)
            # Update the parallel operators list and the entities.
            output['operator'].append(computed['operator'])
            output['entities'].update(computed['entities'])

        return output
    
    # Operator default parameters.
    params = {
        'dag': dag,
        'task_id': operator['id']
    }
    # Update default parameters with individual operator parameters.
    params.update(operator['params'])
    
    # Generate the operator.
    computed_operator = operator['type'](**params)

    # Return operator information.
    return {
        'operator': computed_operator,
        'operator_id': operator['id'],
        'entities': {
            operator['id']: computed_operator
        }
    }

"""
    This function generates a flow by the given tasks.
    
    In this case this function returns the following dictionary,
    with all the related information.
    
        {
            'flow': Operator,   # Generated flow.
            'entities': {}      # All the Operator entities used.
        }
"""
def generate_flow(dag, tasks, dependencies):
    n_tasks = len(tasks)
    
    # There are no tasks, return.
    if n_tasks == 0:
        return
    
    # There is only one task, compute it and return it.
    elif n_tasks == 1:
        computed_operator = generate_operator(dag, tasks[0])
        flow = computed_operator['operator']
        
        if len(dependencies) > 0:
            return {
                'flow': dependencies[0] >> flow,
                'entities': computed_operator['entities']
            }
        
        return {
            'flow': flow,
            'entities': computed_operator['entities']
        }
    else:
        entities = {}
        generated_operator = generate_operator(dag, tasks[0])
        flow = generated_operator['operator']
        
        if len(dependencies) > 0:
            flow = dependencies[0] >> flow
            
        entities.update(generated_operator['entities'])
                
        del tasks[0]
        
        for operator in tasks:
            generated_operator = generate_operator(dag, operator)
            entities.update(generated_operator['entities'])
            flow = flow >> generated_operator['operator']
            
        return {
            'flow': flow,
            'entities': entities
        }

"""
    This function generates the airflow flows.
"""
def generate_airflows(dag, flows):
    n_flows = len(flows)

    if n_flows == 0:
        yield None
    elif n_flows == 1:
        generated_flow = generate_flow(dag, flows[0], [])
        yield generated_flow['flow']
    else:
        entities = {}
        generated_flows = []

        for flow in flows:
            dependencies = []
            if 'depends_on' in flow:
                dependencies.append(entities[flow['depends_on']])
            
            generated_flow = generate_flow(dag, flow['tasks'], dependencies)
            current_flow = generated_flow['flow']
            entities.update(generated_flow['entities'])


            yield current_flow
