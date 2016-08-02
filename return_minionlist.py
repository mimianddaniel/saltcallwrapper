import salt.utils.minions
import salt.config

def matched_minions(tgt, tgt_type, greedy=False, **kwargs):
    saltopts = salt.config.master_config('/etc/salt/master')
    ckminions = salt.utils.minions.CkMinions(saltopts)
    mytgt = tgt.split(',')
    mytgt_type = tgt_type.split(',')
    if len(mytgt) == len(mytgt_type):
        if len(mytgt) == 1:
            minions = ckminions.check_minions(mytgt[0], expr_form=mytgt_type[0], greedy=greedy)
        else:
            minions = {}
            mytgt = mytgt[0:-1]
            mytgt_type = mytgt_type[0:-1]
            for index, item in enumerate(mytgt):
                minions[item] = ckminions.check_minions(item, expr_form=mytgt_type[index], greedy=greedy)
                minions[item + ':count'] = len(minions[item])

        return  minions
