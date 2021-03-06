# saltcallwrapper
saltcallwrapper is a python module that allows you to send command through Salt reactor in salt architect with Syndics and be able to retrieve returns through a returner( redis in this case )

If you manage a large minion environments and have syndics deployed in your setup, you probably ran into issues that doesnt work as expected:

1. publish.publish : thread exhasust issue, and also requires a patch that will work in syndic environment as saltmaster wont have all the PKI of the minions hence call comes back right away without the return from the minins. 
2. Once you have too many minions, one top saltmaster wont be able to handle all the returns, and returns gets dropped.

With async operation like reactor, i had no way of knowing whether I have gotten all the returns from the minions which Salt master is able to do by doing matching of target regex against cache files + PKI on the syndics and itself. I wrote a custom module which is similar to Ckminion Classs from Salt that returns target matched minions. 

The solution i came up with was to use redis returner and have the minions to pull the returns from other minions, which would alleviate overwhelming returns from the minions. By using reactor, I am not holding onto a worker thread to process my event but have it process in a queue which would handle more requests in reasonable matter without exhasuing all the threads on the saltmaster.

##Install
- saltcallwrapper.py is a module and should be avail from where you compiled your Salt python with. 
- salt_wrapper.py is just python script that calls the lib. 
- sls is for placing it on the master for reactor. 

##Requirement
a couple of redis boxes with firewall holes on 6369, obviously this can be changed as you wish. 
reactor setup 
module that returns target matched minions. 

###Example
eg:

```
usage: saltremotecmd.py [-h] [--target TARGET] [--tag TAG] [--timeout TIMEOUT]
                       [--tgt_type TGT_TYPE] [--fun FUN] [--extra EXTRA]
                       [--debug] [--summary] [--output_format OUTPUT_FORMAT]

optional arguments:
  -h, --help            show this help message and exit
  --target TARGET, -T TARGET
                        your target regex: eg I@roles:wepapp
  --tag TAG, -N TAG     name of tag: eg name of tag that is set in the reactor
  --timeout TIMEOUT, -t TIMEOUT
                        timeout for salt
  --tgt_type TGT_TYPE, -S TGT_TYPE
                        type of regex: compound glob pcre
  --fun FUN, -F FUN     your module that will get ran when certain tag is
                        triggered
  --extra EXTRA, -E EXTRA
                        extra argument: needs to be separated by space eg:
                        myname=john sirname=snow hodorisaarg
  --summary             show the end summary
  --output_format OUTPUT_FORMAT, -O OUTPUT_FORMAT
                        format output type
`

`sudo saltremotecmd.py -S compound -T webapp* -N 'rpminfo' -F run -E 'bash'` 
should return in txt otherwise json as provided in arguement 

In my environment, it ususaly runs anywhere between 13 secs - 48 secs for targetting against ~1000.


