## We have 3 values 
# value A
 - Get max value between Historic Fare, Profile Fare and Last sold fare
    : if max value greater than strategy Fare 
        - then: take max value 50% of time and 50% of time upsell by 5%
        - else: avegare of max fare and startegy fare
            - if tolerance value is not null, take difference of strategy fare and tolerance value and allocation based on which is higher than maxfare or startegy fare - tolarence fare

# value B
 - Get max value between Historic Fare, Profile Fare and Last sold fare which below strategy fare
    : if max value = 0 
        - then: take startegy fare by default
        - else: open variance between startegy fare and max value
            - if tolerance value is not null, take difference of strategy fare and tolerance value and allocation based on which is higher than maxfare or startegy fare - tolarence fare

# value C
 - Skip Allocations