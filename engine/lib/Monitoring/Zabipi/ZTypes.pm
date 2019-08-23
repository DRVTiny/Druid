package Monitoring::Zabipi::ZTypes;
use strict;
use 5.16.1;

use List::MoreUtils qw(each_array);

use Exporter qw(import);
our @EXPORT=qw(%zobjTypes);
our @EXPORT_OK=qw(%_sql);

our %zobjTypes=(
    'by_name'=>{
        'item'=>{
            'letter'=>'i',
            'id'=>'itemid',
            'name'=>'name',
            'index'=>0,
            'table'=>'items',
            'xattrs'=>['key_','hostid','delay','type','value_type','state','status'],
            'db'=>'source',
        },
        'trigger'=>{
            'letter'=>'t',
            'id'=>'triggerid',
            'name'=>[],
            'index'=>1,
            'table'=>'triggers',
            'xattrs'=>['status','state','priority','value'],
            'db'=>'source',
            'sql'=>'getListedTrigObjs',
            'redis_conv'=>{'svc-path'=>'CB'},
            'redis_db'=>5,
        },
        'host'=>{
            'letter'=>'h',
            'id'=>'hostid',
            'name'=>['host','name'],
            'index'=>2,
            'table'=>'hosts',
            'xattrs'=>['status','maintenance_status'],
            'db'=>'source',
            'redis_db'=>6,
        },
        'hostgroup'=>{
            'letter'=>'g',
            'id'=>'groupid',
            'name'=>'name',
            'index'=>3,
            'table'=>'groups',
            'xattrs'=>[],
            'db'=>'source',
            'redis_db'=>7,
        },
        'service'=>{
            'letter'=>'s',
            'id'=>'serviceid',
            'name'=>'name',
            'index'=>4,
            'table'=>'services',
            'xattrs'=>['algorithm','triggerid'],
            'db'=>['source','cache'],
            'redis_conv'=>{'dependencies'=>'I', 'parents'=>'I'},
            'redis_db'=>8,
        },
        'application'=>{
            'letter'=>'a',
            'id'=>'applicationid',
            'name'=>'name',
            'index'=>5,
            'table'=>'applications',
            'xattrs'=>['hostid'],
            'db'=>'source',
        },
    },
);

our %_sql = (
    'getActualTrigLostFunK' => {
        'rq' => <<EOSQL,
SELECT DISTINCT
    t.triggerid, 
    IF(
        (t.status+MAX(i.status)) > 0,
            %{LFK_DISABLED},
            IF(t.state > 0,
                %{LFK_UNKNOWN},
                IF(MAX(h.maintenance_status) > 0, -1 , 1) * IF(t.value > 0, t.priority - %{TRIG_PRIO_INFO}, %{LFK_OK})
            )
    ) lostfunk
FROM 
    triggers t
	INNER JOIN functions f 	USING(triggerid)
	INNER JOIN items     i 	USING(itemid)
	INNER JOIN hosts     h  USING(hostid)
        INNER JOIN services  s 	ON t.triggerid=s.triggerid
WHERE
    t.priority > %{TRIG_PRIO_INFO}
GROUP BY t.triggerid
EOSQL
        'db' => 'source'
    },
    'getListedTrigObjs'=>{
        'rq'=><<EOSQL,
SELECT DISTINCT
    t.triggerid,
    t.value,
    IF((t.status+MAX(i.status))>0,-4,IF(t.state>0,-1,IF(t.value>0,t.priority-1,0))) lostfunk,
    t.status,
    t.state,
    t.priority,
    t.lastchange
FROM 
    triggers t
	INNER JOIN functions f 	USING(triggerid)
	INNER JOIN items i 	USING(itemid)
        INNER JOIN services s 	ON t.triggerid=s.triggerid
WHERE
    t.priority>1
GROUP BY
    t.triggerid
EOSQL
        'db'=>'source'
    },    
    'svcGet'=>{
        'rq'=>q(select s.algorithm, s.name, s.serviceid, s.triggerid from services s where s.serviceid=?),
        'db'=>['cache','source']
    },
    'svcGetDeps'=> {
        'rq'=>q(select s.algorithm, s.name, s.serviceid, s.triggerid from services s inner join services_links sl on s.serviceid=sl.servicedownid where sl.serviceupid=?),
        'db'=>['cache','source']        
    },
    'svcGetRootDeps'=> {
        'rq'=>q(select s.algorithm, s.name, s.serviceid, s.triggerid from services s left join services_links sl on s.serviceid=sl.servicedownid and sl.soft=0 where sl.servicedownid is null),
        'db'=>['cache','source'],
    },
    'svcGetIDsOfRootDeps'=> {
        'rq'=>q(select s.serviceid from services s left join services_links sl on s.serviceid=sl.servicedownid and sl.soft=0 where sl.servicedownid is null),
        'db'=>['cache','source'],
    },    
    'zobjGetBySvcid'=> {
        'rq'=>q(select column_json(z.obj) json from services s inner join services_zobjs sl using(serviceid) inner join zobjs z using(z_uoid) where s.serviceid=?), 
        'db'=>'cache'
    },
    'zobjGetByZuoid'=>{
        'rq'=>q(select column_json(z.obj) json from zobjs z where z.z_uoid=?),
        'db'=>'cache',
    },    
    'svcGetAssocZuoid'=>{
        'rq'=>q(select z.z_uoid zuoid from services s inner join services_zobjs sl using(serviceid) inner join zobjs z using(z_uoid) where s.serviceid=?),
        'db'=>'cache',
    },
    'getTrgByID'=>{
        'rq'=>q(select column_json(z.obj) jsontrg from services s inner join services_zobjs sz using(serviceid) inner join zobjs z using(z_uoid) where s.triggerid=?),
        'db'=>'cache',
    }, 
    'getAllSvcsWithDeps'=>{
        'rq'=><<EOSQL,
select
    s.serviceid,
    s.name,
    s.algorithm,
    s.triggerid,
    if(sd.service_deps IS NULL,'',sd.service_deps) dependencies,
    if(sp.service_pars IS NULL,'',sp.service_pars) parents
from
    services s
        left join (
            select sl.serviceupid serviceid, group_concat(sl.servicedownid separator ',') service_deps from services_links sl group by sl.serviceupid
        ) sd using (serviceid)
        left join (
            select sl.servicedownid serviceid, group_concat(sl.serviceupid separator ',') service_pars from services_links sl group by sl.servicedownid
        ) sp using(serviceid)
        left join
            triggers t using(triggerid)
where
    if(t.triggerid is null,1,if(t.priority>1,1,0))=1
EOSQL
        'db'=>'source',
    }
);

my @zoprops=values $zobjTypes{'by_name'};
my @zokeys=keys $zobjTypes{'by_name'};
my $iter=each_array(@zokeys, @zoprops);
while (my ($zname,$zdescr)=$iter->()) {
    $zdescr->{'type'}=$zname;
    push @{$zdescr->{'xattrs'}},ref($zdescr->{'name'})?@{$zdescr->{'name'}}:$zdescr->{'name'}, $zdescr->{'id'};
    $zdescr->{'xattrs'}=[sort @{$zdescr->{'xattrs'}}] if $#{$zdescr->{'xattrs'}};
}

%{$zobjTypes{'by_letter'}}	= map { $_->{'letter'}=>$_ } @zoprops;
%{$zobjTypes{'by_index'}}	= map { $_->{'index'} =>$_ } @zoprops;

1;
