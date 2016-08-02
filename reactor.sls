{% set postdata = data.get('data', {}) %}
{% set mytgt = postdata['data']['tgt'] %}


{% if data['tag'] == 'rpminfo' %}
rpminfo:
    {% if postdata.data.action == 'run' %}
    local.my_lowpkg.list_pkgs:
        - arg:
        {% for myarg in postdata['data']['argument'] %}
            - {{ myarg }}
        {% endfor %}
    {% endif %}
{% elif data['tag'] == 'returnlistminion' %}
returnminion:
    {% if postdata.data.action == 'match' %}
    local.return_minionlist.matched_minions:
        - arg:
        {% for myarg in postdata['data']['argument'] %}
            - {{ myarg }}
        {% endfor %}
    {% endif %}
{% endif %}
        - tgt: {{ mytgt }}
        - user: {{ postdata['data']['user_name'] }}
        - ret: redispubsub
        - expr_form: {{ postdata['data']['tgt_type'] }}
        - kwarg:
            metadata:
                'channel': {{ postdata['data']['channel'] }}
