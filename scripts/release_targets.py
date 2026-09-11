"""Run authorized target pipelines independently. One failure never hides a peer's acceptance."""
from concurrent.futures import ThreadPoolExecutor, as_completed

def selected(targets, action, on_result, max_workers=2):
    if not targets or len(set(targets))!=len(targets) or max_workers<1:
        raise ValueError('unique nonempty targets required')
    results={}
    with ThreadPoolExecutor(max_workers=min(max_workers,len(targets))) as pool:
        futures={pool.submit(action,t):t for t in targets}
        for future in as_completed(futures):
            target=futures[future]
            try:
                result=future.result()
                if not isinstance(result,dict) or 'state' not in result:raise ValueError('target has no factual state')
            except Exception as error:
                result={'state':'needs_recovery','failure_type':type(error).__name__,'recovery':'inspect original target receipts; successful peers remain installed'}
            results[target]=result
            on_result(target,result)
    return {'targets':results,'all_targets_accepted':all(v['state']=='accepted' for v in results.values()),
            'selected_targets':list(targets),'scope':'original target set preserved; independent pipelines'}
