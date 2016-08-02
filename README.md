# saltcallwrapper
Able to send command through Salt reactor in salt architect with Syndics

The goal is to able to submit events from a minion and receive returns from targetted minions thru using returner. 

If you manage large minion environments and have syndics deployed in your setup, you probably ran into issues that doesnt work as expected:
1 publish.publish : thread exhasust issue, and also requires a patch that will work in syndic environment as saltmaster wont have all the PKI of the minions hence call comes back right away without the return from the minins. 
2. Once you have too many minions, one top saltmaster wont be able to handle all the returns, and returns gets dropped.

With async operation like reactor, i had no way of knowing whether I have gotten all the returns from the minions which Salt master is able to do by doing matching of target regex against cache files + PKI on the syndics and itself. I wrote a custom module which is similar to Ckminion Classs from Salt that returns target matched minions. 

The solution i came up with was to use a returner and have the minions to pull the returns from other minions, which would alleviate overwhelming returns from the minions. By using reactor, I am not holding onto a worker thread to process my event but have it process in a queue which would handle more requests in reasonable matter without exhasuing all the threads on the saltmaster.

Requirement:
a couple of redis boxes with firewall holes on 6369, obviously this can be changed as you wish. 
reactor setup 
module that returns target matched minions.


