package database

type GroupLoadBalancerPolicy int

const (
	GroupLoadBalancerPolicy_Random GroupLoadBalancerPolicy = iota
	GroupLoadBalancerPolicy_RoundRobin
	GroupLoadBalancerPolicy_None
	GroupLoadBalancerPolicy_Pool
)

type GroupRotationPolicy int

const (
	GroupRotationPolicy_EverySecond GroupRotationPolicy = iota
	GroupRotationPolicy_EveryHalfMinute
	GroupRotationPolicy_EveryMinute
	GroupRotationPolicy_EveryHour
)
