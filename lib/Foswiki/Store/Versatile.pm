# See bottom of file for license and copyright information

=begin TML

---+ package Foswiki::Store::Versatile

=cut

package Foswiki::Store::Versatile;
use strict;
use warnings;

use Foswiki                                ();
our @ISA = qw(Foswiki::Store);

use Assert;
use Error qw( :try );

use Foswiki::Meta                          ();
use Foswiki::Iterator::NumberRangeIterator ();
use Foswiki::ListIterator                  ();
use Foswiki::Users::BaseUserMapping        ();

use Time::HiRes qw(gettimeofday tv_interval);

{ # Need this block to allow store routines to take advantage of persistent environments

=begin TML

---++ ClassMethod new()

Construct a Store module.

The store does not appear to be persistent. Its part of the Foswiki session object which is created for each request. However, some caching would be better to be by persistence rather than by request as some data is immutable once created. Therefore, $persist is used as a lexical within a closure to keep certain cached values across requests.

=cut

my $persist = undef;

# Used to pre-load some NIDs and FIDs (in conjunction with Meta::Validate)
# If any TYPE in validate has a 'name' key then you cannot generate any FIDs
# as you do not know the actual name
# except for attachFOBS where the name is then always blank and you can
# generate based on that
# With name keys you could generate a META:TYPE{nameseq=000 name=$EoV} type of this, I'm not sure that's worth it though

my %namespace = (
    latestTopics => 0,
    otherTopics => 1, 
    danglingTopics => 2, # SMELL: Needed? what about rev=0
    latestWebs => 3,
    latestVersatileTopics => 4,
    otherVersatileTopics => 5,
    versatileWebs => 6,
    root => 99  # Only one entry, inserted at database creation 
);
# The 'webid' of the root
# During recreate a row must be inserted into FOBinfo with fobid and webid with this value
my $rootId = 1;

# End Of Value to force SQL to store/match trailing spaces
# Need to investigate \0 as well
#    Possible pro: guarantees correct collation?
#              but suggestion that control chars are collated together
#              but needs double checking
#    Possible con: Not whitespace, final word for FTS broken? apparently not, so \0 it is
#
my $EoV = "\0";

my $NIDconstants = 100000000; # Pre-defined NIDs here
my $NIDbase = $NIDconstants + 10000; # Dynamically created NIDS start here
my $name000 = substr($NIDconstants,1);
my $nameSeqLen = length($name000); # No of digits in a nameSeq
my $nameSeqFormat = sprintf("%%0%sd$EoV",$nameSeqLen); # Note nameSeq has lead 0s
my $nameSeqRegex = qr/\A[0-9]{$nameSeqLen}[$EoV]\z/;
my $nameLen = 180;


my $otherVersionBit = 0x20;
my $otherVersionMask = 0x1f;

my $textFID;

my $NIDtype = "int unsigned";
my $fobIdType = "int unsigned";          
my $FIDtype = 'int unsigned';
my $versionType = 'int unsigned';
my $valuesType = 'longtext';

sub _new_persistentRun {
    # This is called by new (see below) if $persist is undef (i.e. first time only). If I could call this very early on during script start-up then I could potentially do more persistent stuff (e.g. pre-loading/caching the fields table) during idle time (before 1st request). It would require a flag to indicate early start rather than just 1st request.
    
    # Note that if this store is instantiated more than once then each object
    # created will share this persistent information. This is by design after all a persistent run benefits from handling multiple requests in one run. Each request will instantiate a new versatile, but that request can re-use earlier 'persistent' info.
    
    # If you intend (for some reason) to maintain two or more active versatile objects, then as the persistent info are things like: the DB connection, caches of immutable tables and caches of prepared queries (also immutable); this is probably fine but please bear it in mind.
    
    # In this block, all $persist{xxx} = ... is effectively $this{xxx} = ..., but saved across requests in a persistent environment
    #
    # Apart from this sub and some use in new(), $persist should not be used but $this (or $store or $versatile etc). That's to ease the possiblity that during design changes something persistent becomes non-persistent and vice versa
    # 
    # So, do expensive things 1st time here, unless some laziness is called for
    my ($this) = (@_);
    $persist = {};

    my $t0 = [ gettimeofday ];
    $persist->{dbh} = DBI->connect_cached( #smell Now I'm using connect_cached this should be moved elsewhere
        "$this->{cfg}{Versatile}{connection};mysql_enable_utf8=1", 
        $this->{cfg}{Versatile}{dbuser},
        $this->{cfg}{Versatile}{dbpass},
        # Errors should not occur (by design) so raise exception (should only be development bugs)
        # Elsewhere versatile must deal with 0 records returned when more expected and vice versa
        # but these are not error conditions
        { RaiseError => 1, AutoCommit => 0 }
    );
    #print "" . $persist->{dbh} . "\n";

    my $int0 = tv_interval($t0, [gettimeofday]);
    #print "Connect in $int0 s\n";

    my $t1 = [ gettimeofday ];
    
    $persist->{FID} = {};
    $persist->{FIDinfo} = {};
    
    $persist->{NID} = {};
    $persist->{name} = {};
    my $fdx = 1;
    my $setFID = sub {
        my ($hasName, $types, $name, $keys, $fieldType) = @_;
        $types = [ $types ] if ref($types) ne 'ARRAY';
        for my $type (@{$types}) {
            my $etype = "$type$EoV";
            my $ename = "$name$EoV";
            for my $key (@{$keys}) {
                my $ekey = "$key$EoV";
                $persist->{FID}{$hasName}{$etype}{$ename}{$ekey} =
                    [$fdx, $fieldType];
                $persist->{FIDinfo}{$fdx++} =
                    [$hasName, $etype, $ename, $ekey, $fieldType];
            }
        }
    };

    # The following are loaded into the persistent cache without going to
    # the DB as such they will be constant and immutable once in the wild.
    # That means that not only can we not change the values below but
    # we cannot add or remove any of them as soon as Versatile is published
    # for the first time.
    
    # If you did add a value later you would need to be sure that no user of
    # Versatile had added that value, because if they have done then adding
    # it here will be a duplicate key with a different value
    
    # Removing a value (and leaving other kv pairs as-is) might be OK, but
    # it's of little value and not worth the risk
    
    # Note that all of these (and following FID constants) are added to the
    # DB. Queries may still be required that refer to them. In addition, 
    # the intent is to maintain referential integrity even though this may
    # only be used during development as an 'assert'

    my @NIDs = ( '', qw(
        _acl
        _local
        _PREF_SET
        _PREF_LOCAL
        _set
        _text
        _web
        attachment
        attr
        attributes
        author
        autoattached
        by
        comment
        CREATEINFO
        date
        definingTopic
        encoding
        FIELD
        FILEATTACHMENT
        FORM
        format
        from
        mandatory
        moveby
        movedto
        movedwhen
        movefrom
        name
        path
        PREFERENCE
        reprev
        rev
        size
        stream
        title
        tmpFilename
        to
        tooltip
        TOPICINFO
        TOPICMOVED
        TOPICPARENT
        type
        user
        value
        version
        WORKFLOW
        WORKFLOWHISTORY
        DENYWEBVIEW
        ALLOWWEBVIEW
        DENYTOPICVIEW
        ALLOWTOPICVIEW
        DENYWEBCHANGE
        ALLOWWEBCHANGE
        DENYTOPICCHANGE
        ALLOWTOPICCHANGE
    ));
    my $ndx = $NIDconstants;
    for my $name (@NIDs) {
        $persist->{NID}{"$name$EoV"} = $ndx;
        $persist->{name}{$ndx++} = "$name$EoV";
    }
    
    for my $NID (1..300) {
        my $name = sprintf($nameSeqFormat, $NID - 1);
        $persist->{NID}{$name} = $NID;
        $persist->{name}{$NID} = $name;
    }

    # notValues are not stored on standard values_... tables
    #           and *may* be stored especially on other tables
    #           (e.g. TOPICINFO of FOBinfo table)
    # isValues  are stored on values_... tables
    # epochDate are stored on values_... tables with the original
    #           string on values_text
    #           the epoch number on values_double
    #           the equivalent dtae on values_datetime
    #           This maintains backwards compatibility and allows direct
    #           datetime based queries

    my ($notValue, $isValue, $epochDate) = (0, 1, 2);
    
    # FIDs also immutable once released. See comments ref NID constants above
    $setFID->(0, '_text', '', [''], $isValue);
    $textFID = $persist->{FID}{0}{"_text$EoV"}{$EoV}{$EoV}[0];

    # These notValues are stored instead on the FOBinfo table
    $setFID->(0, [ qw( TOPICINFO CREATEINFO ) ], '', 
        [ qw( author version date comment reprev ) ],
        $notValue
    );
    # These notValues are *NOT* stored by versatile: they do not apply
    $setFID->(0, [ qw( TOPICINFO CREATEINFO ) ] , '', 
        [ qw( format rev encoding ) ],
        $notValue
    );

    $setFID->(0, 'TOPICMOVED', '', 
        [ qw( from to by ) ],
        $isValue
    );
    $setFID->(0, 'TOPICMOVED', '', 
        [ qw( date ) ],
        $epochDate
    );
    $setFID->(2, 'TOPICPARENT', $name000, 
        [ qw( name ) ],
        $isValue
    );

    # Because this is an attachFOB, therefore one FILEATTACHMENT per FOB
    # The name is always name sequence 0 and we can create FIDs with a
    # name = ''
    $setFID->(2, 'FILEATTACHMENT', $name000, 
        [ qw( name ) ],
        $isValue
    );
    $setFID->(0, 'FILEATTACHMENT', '', 
        [ qw( version path size user comment attr ) ],
        $isValue
    );
    $setFID->(0, 'FILEATTACHMENT', '', 
        [ qw( date ) ],
        $epochDate
    );

    $setFID->(2, 'FORM', $name000, 
        [ qw( name ) ],
        $isValue
    );
    $setFID->(2, 'FIELD', $name000, 
        [ qw( name ) ], # No ( value title ) need real name to complete FID
        $isValue
    );
    $setFID->(2, 'PREFERENCE', $name000, 
        [ qw( name ) ], # No ( value type ) need real name to complete FID
        $isValue
    );
    $setFID->(2, '_PREF_SET', $name000, 
        [ qw( name ) ], # No ( value type ) need real name to complete FID
        $isValue
    );
    $setFID->(2, '_PREF_LOCAL', $name000, 
        [ qw( name ) ], # No ( value type ) need real name to complete FID
        $isValue
    );
    my $int1 = tv_interval($t1, [gettimeofday]);
    # print "NID/FID cache in $int1 s\n";
}

sub new {
    my $class = shift;
    my $this  = $class->SUPER::new(@_);

    if( $this->{cfg}{Versatile}{persistent} ) {
        $this->_new_persistentRun() if !defined $persist;;
    }
    else {
        $this->_new_persistentRun();
    }
    # Load (1st time) or reload (all other times) persistent entries (e.g mod_perl or FastCGI)
    # Non persistent is always 1st time (plain old CGI)
    @{$this}{keys %$persist} = values %$persist; # Fast copy of $persist entries into $this

    # Following are non persistent store values that cannot be persistent, reset on every request
    #$Foswiki::Meta::VALIDATE{FILEATTACHMENT}{attachFOB} = 1;
    $this->{errstr} = "Connect OK";

    $this->{Number} = qr/^\s*?[-+]?[0-9]*\.?[0-9]+([eE][-+]?[0-9]+)?\s*?$/o;

    $this->{webName} = undef;
    
    return $this;
}

=begin TML

---++ ObjectMethod finish()
Break circular references.

=cut

# Note to developers; please undef *all* fields in the object explicitly,
# whether they are references or not. That way this method is "golden
# documentation" of the live fields in the object.

# I am not undeffing all fields deliberately because of $persist, see above
# Mind you I need to think about this

# In particular I am *not* disconnecting the database, but I am performing a commit. At least in a CGI environment any updates will be made (and a persistent as well for that matter)

sub finish {
    my $this = shift;
#    $this->_insertFlush(1); # Only relevant during upload why is this finish not called at program exit?
    $this->{dbh}->commit;
    $this->SUPER::finish();
}

=begin TML

---++ ObjectMethod readTopic($topicObject, $version) -> ($rev, $isLatest)
   * =$topicObject= - Foswiki::Meta object
   * =$version= - revision identifier, or undef
Reads the given version of a topic, and populates the =$topicObject=.
If the =$version= is =undef=, or there is no revision numbered =$version=, then
reads the most recent version.

Returns the version identifier of the topic that was actually read. If
the topic does not exist in the store, then =$rev= is =undef=. =$isLatest=
will  be set to true if the version loaded (or not loaded) is the
latest available version.

=cut

# SMELL: there is no way for a consumer of Store to determine if
# a specific revision exists or not.

sub readTopic {
    my ( $this, $meta, $version ) = @_;

    my $ti = $this->_topicInfo($meta->web, $meta->topic, $version);
    return undef if !defined $ti;
    my $fobId = $ti->{fobid};

    $meta->{TOPICINFO}[0]{author} = $ti->{user};
    $meta->{TOPICINFO}[0]{date} = $ti->{date};
    $meta->{TOPICINFO}[0]{version} = $ti->{version};
    $meta->{TOPICINFO}[0]{comment} = $ti->{comment};
    $meta->{TOPICINFO}[0]{reprev} = $ti->{reprev} if $ti->{reprev};

    $meta->{_text} = '';
        
    my $sth = $this->{dbh}->prepare_cached(
        "select ducktype, FID, value " . 
            "from values_text " . 
            "where webid = ? and fobid = ? " .
            "order by ducktype"
    );
    $sth->execute($ti->{webid}, $fobId);
    my $rows = $sth->fetchall_arrayref();
    $meta->{_indices} = my $ind = {};

    $this->{missing} = {};
    for my $row (@$rows) {
        my (undef, $FID) = @{$row};
        $this->{missing}{$FID} = 1 if !defined $this->{FIDinfo}{$FID};
    }
    my @FIDs = keys %{$this->{missing}};
    if(@FIDs) {
        my $sql = "select FID, fieldType, hasName, " .
            "typeNID, type.name as etype, " . 
            "nameNID, name.name as ename, " . 
            "keyNID, keyT.name as ekey " .
            "from fields, names type, names name, names keyT " .
            "where FID in (" .
            substr(('?,' x scalar(@FIDs), 0, -1)) .
            ") " .
            "and typeNID = type.NID " .
            "and nameNID = name.NID " . 
            "and keyNID = keyT.NID";
        my $sth = $this->{dbh}->prepare($sql);
        $sth->execute(@FIDs);
        my $rows = $sth->fetchall_arrayref( {} );
        
        for my $row (@{$rows}) {
            $this->_cacheName($row->{typeNID}, $row->{etype});
            $this->_cacheName($row->{nameNID}, $row->{ename});
            $this->_cacheName($row->{keyNID},  $row->{ekey});

            my ($type, $name, $key) =
                ($row->{etype}, $row->{ename}, $row->{ekey});
            my $FID = $row->{FID};
            my $hasName = $row->{hasName};
            my $fieldType = $row->{fieldType};

            $this->{FID}{$hasName}{$type}{$name}{$key} = [$FID, $fieldType ];
            $this->{FIDinfo}{$FID}=[$hasName, $type, $name, $key,$fieldType];
        }
    }
    
#    print "=== $meta->{_web}.$meta->{_topic} =======" . 
#        "===============================\n";
    # SMELL: saveTopic via fixUp_PREFS actually assigns an array ref, we need a hash ref, anyway maybe good idea to clear $meta of junk 
    for my $row (@$rows) {
        my ($duck, $FID, $valu) = @{$row};
        $duck &= $otherVersionMask; # current/other N/A reading FOB values
        chop($valu);
        my ($hasName, $etype, $ename, $ekey, $fType) =
            @{$this->{FIDinfo}{$FID}};

        my $type = substr($etype, 0, -1);
        my $name = substr($ename, 0, -1);
        my $key  = substr($ekey,  0, -1);
        
        if($duck == 0) { 
            die "hasName not == 2!!\n" if $hasName != 2; #SMELL -> assert
            my $seq = substr($name, 0, -1);
            $seq = $name + 0;
            $ind->{$type}{$valu} = $seq;
            $meta->{$type}[$seq]{name} = $valu;
        }
        else {
            if($type eq '_text') {
                $meta->{_text} = $valu;
            }
            else {
                $meta->{$type}
                    [ $hasName ? $ind->{$type}{$name} : 0 ]
                    { $key } = $valu;
            }
        }
    }

    my %pref_set   = ref $meta->{_PREF_SET}   eq 'ARRAY' ? map { $_->{name} => $_->{value} } @{$meta->{_PREF_SET}}   : ();
    my %pref_local = ref $meta->{_PREF_LOCAL} eq 'ARRAY' ? map { $_->{name} => $_->{value} } @{$meta->{_PREF_LOCAL}} : ();

    $meta->{_PREF_SET}   = \%pref_set;
    $meta->{_PREF_LOCAL} = \%pref_local;

    $meta->setLoadStatus($ti->{version}, $ti->{namespace} == $namespace{latestTopics});
    return ($ti->{version}, $ti->{namespace} == $namespace{latestTopics});
}

sub readTopicsEnMasse {
    my ( $this, $metaList ) = @_;
    
    my @fobids;
    my %metaByFobId;
    for my $meta (@{$metaList}) {
        my $ti = $this->_topicInfo($meta->web, $meta->topic, 0);
        next if !defined $ti;
        push @fobids, $ti->{fobid};
#        push @fobids, $ti->{webid}, $ti->{fobid};
        $metaByFobId{$ti->{fobid}} = $meta;

        $meta->setLoadStatus(
            $ti->{version},
            $ti->{namespace} == $namespace{latestTopics}
        );
        $meta->{_indices} = {};

        $meta->{TOPICINFO}[0]{author} = $ti->{user};
        $meta->{TOPICINFO}[0]{date} = $ti->{date};
        $meta->{TOPICINFO}[0]{version} = $ti->{version};
        $meta->{TOPICINFO}[0]{comment} = $ti->{comment};
        $meta->{TOPICINFO}[0]{reprev} = $ti->{reprev} if $ti->{reprev};
    }
    return if !@fobids;

    my $sth = $this->{dbh}->prepare_cached(
        "select ducktype, FID, value, fobid " . 
            "from values_text " . 
            "where fobid in (". substr('?,' x scalar(@fobids),0,-1) .") " .
#            "where " . substr('(webid = ? and fobid = ?) or ' x (scalar @fobids / 2),0,-3) .
            "order by webid, fobid, ducktype"
    );
    $sth->execute(@fobids);
    my $rows = $sth->fetchall_arrayref();

    $this->{missing} = {};
    for my $row (@$rows) {
        my (undef, $FID) = @{$row};
        $this->{missing}{$FID} = 1 if !defined $this->{FIDinfo}{$FID};
    }
    my @FIDs = keys %{$this->{missing}};
    if(@FIDs) {
        my $sql = "select FID, fieldType, hasName, " .
            "typeNID, type.name as etype, " . 
            "nameNID, name.name as ename, " . 
            "keyNID, keyT.name as ekey " .
            "from fields, names type, names name, names keyT " .
            "where FID in (" .
            substr(('?,' x scalar(@FIDs), 0, -1)) .
            ") " .
            "and typeNID = type.NID " .
            "and nameNID = name.NID " . 
            "and keyNID = keyT.NID";
        my $sth = $this->{dbh}->prepare($sql);
        $sth->execute(@FIDs);
        my $rows = $sth->fetchall_arrayref( {} );
        
        for my $row (@{$rows}) {
            $this->_cacheName($row->{typeNID}, $row->{etype});
            $this->_cacheName($row->{nameNID}, $row->{ename});
            $this->_cacheName($row->{keyNID},  $row->{ekey});

            my ($type, $name, $key) =
                ($row->{etype}, $row->{ename}, $row->{ekey});
            my $FID = $row->{FID};
            my $hasName = $row->{hasName};
            my $fieldType = $row->{fieldType};

            $this->{FID}{$hasName}{$type}{$name}{$key} = [$FID, $fieldType ];
            $this->{FIDinfo}{$FID}=[$hasName, $type, $name, $key,$fieldType];
        }
    }
    
    for my $row (@$rows) {
        my ($duck, $FID, $valu, $fobid) = @{$row};
        $duck &= $otherVersionMask; # current/other N/A reading FOB values
        chop($valu);
        my ($hasName, $etype, $ename, $ekey, $fType) =
            @{$this->{FIDinfo}{$FID}};

        my $type = substr($etype, 0, -1);
        my $name = substr($ename, 0, -1);
        my $key  = substr($ekey,  0, -1);
        
        my $meta = $metaByFobId{$fobid};
        my $ind = $meta->{_indices};

        if($duck == 0) { 
            die "hasName not == 2!!\n" if $hasName != 2; #SMELL -> assert
            my $seq = substr($name, 0, -1);
            $seq = $name + 0;
            $ind->{$type}{$valu} = $seq;
            $meta->{$type}[$seq]{name} = $valu;
        }
        else {
            if($type eq '_text') {
                $meta->{_text} = $valu;
            }
            else {
                $meta->{$type}
                    [$hasName ? $ind->{$type}{$name} : 0 ]
                    {$key} = $valu;
            }
        }
    }
}

=begin TML

---++ ObjectMethod moveAttachment( $oldTopicObject, $oldAttachment, $newTopicObject, $newAttachment  )
   * =$oldTopicObject, $oldAttachment= - spec of attachment to move
   * $newTopicObject, $newAttachment= - where to move to
Move an attachment from one topic to another.

The caller to this routine should check that all topics are valid, and
access is permitted.

=cut

=begin TML

---++ ObjectMethod copyAttachment( $oldTopicObject, $oldAttachment, $newTopicObject, $newAttachment  )
   * =$oldTopicObject, $oldAttachment= - spec of attachment to copy
   * $newTopicObject, $newAttachment= - where to move to
Copy an attachment from one topic to another.

The caller to this routine should check that all topics are valid, and
access is permitted.

=cut

=begin TML

---++ ObjectMethod attachmentExists( $topicObject, $att ) -> $boolean

Determine if the attachment already exists on the given topic

=cut

=begin TML

---++ ObjectMethod moveTopic(  $oldTopicObject, $newTopicObject, $cUID )

All parameters must be defined and must be untainted.
    I take this to mean that the store can rely on these params

What should the store do if:
    $oldTopicObject does not exist
    $newTopicObject already exists
    
Are the web, topic attrs of $meta normalised already?

=cut

# Needs work to divide labour between Versatile and parent
sub moveTopic {
    my ( $this, $oldTopicObject, $newTopicObject, $cUID ) = @_;

    $this->{missing} = {};
    my ($oldWeb, $oldTopic) = ($oldTopicObject->web, $oldTopicObject->topic);
    my ($newWeb, $newTopic) = ($newTopicObject->web, $newTopicObject->topic);
    
    $oldWeb =~  s#\.#/#go;
    $newWeb =~  s#\.#/#go;
    
    $this->_addNames("$oldWeb$EoV", "$oldTopic$EoV",
                     "$newWeb$EoV", "$newTopic$EoV");
    $this->_readNames();   # Select names not cached and populate caches
    $this->_insertNames(); # Insert any names not already on DB
    $this->_readNames();   # Select new inserted names and cache them

    my $sql =
        "update FOBinfo " .
            "where webid = " .
            "(select fobid from FOBinfo " . 
                "where namespace = $namespace{latestWebs} " .
                "and webid = $rootId " .
                "and NID = ?" .
            ") " .
            "and NID = ? " .
            "set webid = ?, " . 
            "set NID = (select NID from names where name = ?)";
    $this->{dbh}->do($sql, {},
        $this->{NID}{"$oldWeb$EoV"}, $this->{NID}{"$oldTopic$EoV"},
        $this->{NID}{"$newWeb$EoV"}, $this->{NID}{"$newTopic$EoV"});
        
    # What about: locks, lease: that's up to FW to make 
    # calls to relevant methods to update
    #
    # Other tables are fobid based so N/A or changes which as a log
    # must not be changed after the fact
        
    # Call parent to move attachments and attachment histories
    $this->SUPER::moveTopic($oldTopicObject, $newTopicObject, $cUID);
}

=begin TML

---++ ObjectMethod moveWeb( $oldWebObject, $newWebObject, $cUID )

Move a web.

What should the store do if:
    $oldWebObject does not exist
    $newWebObject already exists

=cut

sub moveWeb {
    my ( $this, $oldWebObject, $newWebObject, $cUID ) = @_;

    $this->{missing} = {};
    my $oldWeb = $oldWebObject->web;
    my $newWeb = $newWebObject->web;
    
    $oldWeb =~  s#\.#/#go;
    $newWeb =~  s#\.#/#go;
    
    $this->_addNames("$oldWeb$EoV", "$newWeb$EoV");
    $this->_readNames();   # Select names not cached and populate caches
    $this->_insertNames(); # Insert any names not already on DB
    $this->_readNames();   # Select new inserted names and cache them

    my $sql =
        "update FOBinfo " .
            "where namespace = $namespace{latestWebs} " .
            "and webid = $rootId " .
            "and NID = ? " .
            "set NID = ?"; 
    $this->{dbh}->do($sql, {},
        $this->{NID}{"$oldWeb$EoV"}, $this->{NID}{"$newWeb$EoV"});
        
    # Call parent to move Web level attachments and attachment histories
    $this->SUPER::moveWeb($oldWebObject, $newWebObject, $cUID);
}

=begin TML

---++ ObjectMethod testAttachment( $topicObject, $attachment, $test ) -> $value

Performs a type test on the given attachment file.
    * =$attachment= - name of the attachment to test e.g =lolcat.gif=
    * =$test= - the test to perform e.g. ='r'=

The return value is the value that would be returned by the standard
perl file operations, as indicated by $type

    * r File is readable by current user (tests Foswiki permissions)
    * w File is writable by current user (tests Foswiki permissions)
    * e File exists.
    * z File has zero size.
    * s File has nonzero size (returns size).
    * T File is an ASCII text file (heuristic guess).
    * B File is a "binary" file (opposite of T).
    * M Last modification time (epoch seconds).
    * A Last access time (epoch seconds).

Note that all these types should behave as the equivalent standard perl
operator behaves, except M and A which are independent of the script start
time (see perldoc -f -X for more information)

Other standard Perl file tests may also be supported on some store
implementations, but cannot be relied on.

Errors will be signalled by an Error::Simple exception.

=cut

=begin TML

---++ ObjectMethod openAttachment( $topicObject, $attachment, $mode, %opts  ) -> $text

Opens a stream onto the attachment. This method is primarily to
support virtual file systems, and as such access controls are *not*
checked, plugin handlers are *not* called, and it does *not* update the
meta-data in the topicObject.

=$mode= can be '&lt;', '&gt;' or '&gt;&gt;' for read, write, and append
respectively. %

=%opts= can take different settings depending on =$mode=.
   * =$mode='&lt;'=
      * =version= - revision of the object to open e.g. =version => 6=
   * =$mode='&gt;'= or ='&gt;&gt;'
      * no options
Errors will be signalled by an =Error= exception.

=cut

=begin TML

---++ ObjectMethod getRevisionHistory ( $topicObject [, $attachment]  ) -> $iterator
   * =$topicObject= - Foswiki::Meta for the topic
   * =$attachment= - name of an attachment (optional)
Get an iterator over the list of revisions of the object. The iterator returns
the revision identifiers (which will usually be numbers) starting with the most
recent revision.

MUST WORK FOR ATTACHMENTS AS WELL AS TOPICS

If the object does not exist, returns an empty iterator ($iterator->hasNext() will be
false).

Uses $topicObject as web/topic address.

=cut

sub getRevisionHistory {
    my ( $this, $meta, $attachment ) = @_;

    return $this->SUPER::getRevisionHistory($meta, $attachment)
        if $attachment;

    my $ti = $this->_topicInfo($meta->web, $meta->topic);
    return Foswiki::Iterator::NumberRangeIterator->new($ti->{version}, 1);
}

=begin TML

---++ ObjectMethod getNextRevision ( $topicObject  ) -> $revision
   * =$topicObject= - Foswiki::Meta for the topic
Get the ientifier for the next revision of the topic. That is, the identifier
for the revision that we will create when we next save.

=cut

# SMELL: There's an inherent race condition with doing this, but it's always
# been there so I guess we can live with it.
sub getNextRevision {
    my ( $this, $meta ) = @_;
    my $ti = $this->_topicInfo($meta->web, $meta->topic);
    return $ti->{version} + 1;
}

=begin TML

---++ ObjectMethod getRevisionDiff ( $topicObject, $rev2, $contextLines  ) -> \@diffArray

Get difference between two versions of the same topic. The differences are
computed over the embedded store form.

Return reference to an array of differences
   * =$topicObject= - topic, first revision loaded
   * =$rev2= - second revision
   * =$contextLines= - number of lines of context required

Each difference is of the form [ $type, $right, $left ] where
| *type* | *Means* |
| =+= | Added |
| =-= | Deleted |
| =c= | Changed |
| =u= | Unchanged |
| =l= | Line Number |

=cut

sub getRevisionDiff {
    my ( $this, $meta, $rev2, $contextLines ) = @_;

    my $rev1 = $meta->getLoadedRev();
    my @list;
    $this->readTopic($meta,$rev1);
    my $text1 = $meta->getEmbeddedStoreForm();
    my $meta2 = Foswiki::Meta->new(
        $Foswiki::Plugins::Session,
        $meta->web, $meta->topic
    );
    $this->readTopic($meta2, $rev2);
    my $text2 = $meta2->getEmbeddedStoreForm();

    my $lNew = _split($text1);
    my $lOld = _split($text2);
    require Algorithm::Diff;
    my $diff = Algorithm::Diff::sdiff( $lNew, $lOld );

    foreach my $ele (@$diff) {
        push @list, $ele;
    }
    return \@list;
}

=begin TML

---++ ObjectMethod getVersionInfo($topicObject, $rev, $attachment) -> \%info

Get revision info of a topic or attachment.
   * =$topicObject= Topic object, required
   * =$rev= revision number. If 0, undef, or out-of-range, will get info
     about the most recent revision.
   * =$attachment= (optional) attachment filename; undef for a topic
Return %info with at least:
| date | in epochSec |
| user | user *object* -- I think that means cUID |
| version | the revision number |
| comment | comment in the VC system, may or may not be the same as the comment in embedded meta-data |

=cut

# Formerly known as getRevisionInfo.
sub getVersionInfo {
    my ( $this, $meta, $version, $attachment ) = @_;
    
    return $this->SUPER::getVersionInfo($meta, $version, $attachment)
        if $attachment;
    
    my $info = $this->_topicInfo($meta->web, $meta->topic, $version);
    return undef if !defined $info;
    
    return {
        date => $info->{date},
        user => $info->{user},
        version => $info->{version},
        reprev => $info->{reprev},
        comment => $info->{comment}
    } if !defined $attachment;   
}

=begin TML

---++ ObjectMethod saveAttachment( $topicObject, $attachment, $stream, $cUID, \%options ) -> $revNum
Save a new revision of an attachment, the content of which will come
from an input stream =$stream=.
   * =$topicObject= - Foswiki::Meta for the topic
   * =$attachment= - name of the attachment
   * =$stream= - input stream delivering attachment data
   * =$cUID= - user doing the save
   * =\%options= - Ref to hash of options
=\%options= may include:
   * =forcedate= - force the revision date to be this (epoch secs) *X* =forcedate= must be equal to or later than the date of the most recent revision already stored for the topic.
   * =minor= - True if this is a minor change (used in log)
   * =comment= - a comment associated with the save
Returns the number of the revision saved.

Note: =\%options= was added in Foswiki 1.2

=cut

=begin TML

---++ ObjectMethod saveTopic( $topicObject, $cUID, $options  ) -> $integer

Save a topic or attachment _without_ invoking plugin handlers.
   * =$topicObject= - Foswiki::Meta for the topic
   * =$cUID= - cUID of user doing the saving. Permission already checked
   * =$options= - Ref to hash of options
=$options= may include:
   * =forcedate= - force the revision date to be this (epoch secs)
    *X* =forcedate= must be equal to or later than the date of the most
    recent revision already stored for the topic.
   * =minor= - True if this is a minor change (used in log)
   * =comment= - a comment associated with the save
   
During an upload/update process force these values
upload is the initial crossover from an existing store
update is for refreshing of content: plugin topics, FW upgrade (which will
probably need to delete existing content before updating with new content)
   * =_version= - the revision to create
   * =_latest= - this is the latest version
   * =_repRev= - to perform the the repRev sub functionality

Is this redundant? (Doesn't FW call save/repRev as apt anyway)
   * =forcenewrevision= - force a new revision even if one isn't needed


Returns the new revision identifier.

Implementation must invoke 'update' on event listeners.

=cut

sub saveTopic {
    my ( $store, $meta, $cUID, $options ) = @_;
    
    my $metaText = $meta->getEmbeddedStoreForm();

    $store->_fixUp_PREFs($meta);

    my $web = $meta->web;
    return 0 unless defined $web && $web ne '';
    $web =~ s#\.#/#go;
    my $topic = $meta->topic;
    return 0 unless defined $topic && $topic ne '';

    $store->{missing} = {};
    $store->_addNames("$web$EoV", "$topic$EoV", "$cUID$EoV");
    
    # Create array of access_id and rule pairs (only one array, not a of a)
    # While at it call _addNames on all rules and access_Ids
    my $rules = $store->_fixUp_ACLs($meta); # This *MUST NOT* be deferred to when we actually update the access table. We capture more NIDs and FIDs here

    my $collectNameNIDs = sub {
        my ($type, $name, $nameSeq) = @_;

        $store->_addNames($type);
        if(defined $name &&
            !$Foswiki::Meta::VALIDATE{substr($type,0,-1)}{attachFOB}) {    
            $store->_addNames($name);
            $store->_addNames($nameSeq);
        }
    };            
    my $collectMetaNIDs = sub {
        my ($type, $name, $key, $valu) = @_;
        $store->_addNames($key);
        $store->_addNames("$valu$EoV") if $type eq "TOPICINFO$EoV" 
                      && ($key eq "author$EoV" || $key eq "comment$EoV")
    };
    _forEachField($meta, $collectNameNIDs, $collectMetaNIDs);

    $store->_readNames();   # Select names not cached and populate caches
    $store->_insertNames(); # Insert any names not already on DB
    $store->_readNames();   # Select new inserted names and cache them
    # At this point the internal NID/name cache will contain all the
    # names required for this save operation

    my $topicNID = $store->{NID}{"$topic$EoV"};
    my $webNID = $store->{NID}{"$web$EoV"};
    my $cUIDNID = $store->{NID}{"$cUID$EoV"};

    my $commentNID = $options->{comment};
    $commentNID = '' if !defined $commentNID;
    $commentNID = $store->{NID}{"$commentNID$EoV"};
    
    $store->{missing} = {};
    my $collectNameFIDs = sub {
        my ($type, $name, $nameSeq) = @_;

        if(!defined $name
            || $Foswiki::Meta::VALIDATE{substr($type,0,-1)}{attachFOB})
        {
            $store->_addFID(2, $type, "$name000$EoV", "name$EoV");
        }
        else {
            $store->_addFID(2, $type, $nameSeq, "name$EoV");
        }
    };
    my $collectMetaFIDs = sub {
        my ($type, $name, $key) = @_;
        if(defined $name
        && !$Foswiki::Meta::VALIDATE{substr($type,0,-1)}{attachFOB}) {
            $store->_addFID(1, $type, $name, $key);
        }
        else {
            $store->_addFID(0, $type, $EoV, $key);
        }
    };
    _forEachField($meta, $collectNameFIDs, $collectMetaFIDs );
    
    $store->_readFIDinfo();
    $store->_insertFIDinfo();
    $store->_readFIDinfo();
    $store->{dbh}->commit;
    # At this point the internal NID/name cache will contain all the
    # names required for this save operation

    my $webid = $store->_webId($web);

    if(!$webid) {
        # web not found, so let's create the web FOB required
        my $blankNID = $persist->{NID}{$EoV}; # NID for ''
        my $sql = "insert into FOBinfo " .
            "(namespace, webid, NID, version, author, comment, date) " .
            "values (?, ?, ?, ?, ?, ?, now())";
        $store->{dbh}->do($sql, {},
            $namespace{latestWebs}, $rootId, $webNID,
            1, $cUIDNID, $blankNID);
        # note fobid of new web aka webid
        $webid = $store->{dbh}->last_insert_id(undef,undef,undef,undef);
        $store->{webName}{$webid} = $web;
        $store->{webId}{$web} = $webid;
    }
   
    # Get fobid (and info) of the latest topic rev
    # Generally, I need $version to store $version+1
    # for repRev I don't need $version, but I do need latest fobid to
    # overwrite (delete, then insert actually)

    my $version; # Version no, we will insert, e.g existing version+1
    my ($thisFobid, $fobid, $updateFobid);

    if($options->{_version}) { # Explicit version for upload
        $version = $options->{_version};
    }
    else {
        my $sth = $store->{dbh}->prepare_cached(
            "select fobid, version from FOBinfo " .
                "where namespace = $namespace{latestTopics} " .
                "and webid = ? and NID = ?"
        );
        $sth->execute($webid, $topicNID);
        my $fobids = $sth->fetchall_arrayref();   
        
        if(@{$fobids}) {
            ($thisFobid, $version) = @{$fobids->[0]};
            $version++;
        }
        else {
            # Topic with this web.topic not found, we need a new one
            # _repRev is false, Force save new topic rev=1
            # do not delete before insert
            $options->{_repRev} = 0; 
            $version = 1; # Need to create 1st FOB for this topic 
        }
    }

    my $space;
    my $now = $options->{forcedate} || time;

    if($options->{_repRev}) {
        $fobid = $thisFobid; # repRev so re-use existing fob
        my $sql = "update FOBinfo where fobid = ?" .
            "set author = ?, " . 
            "set comment = ?, " . 
            "set date = from_unixtime(?)";
        my @places = ($fobid, $cUIDNID, $commentNID, $now);
        $store->{dbh}->do($sql, {}, @places);
        $space = $namespace{latestTopics};
    }
    else { # Save/upload therefore new FOB required
        my $sql = "insert into FOBinfo " .
            "(namespace, webid, NID, version, author, comment, date) " .
            "values (?, ?, ?, ?, ?, ?, from_unixtime(?))";
        $space = $options->{_latest}
                ? $namespace{latestTopics}
                : $namespace{otherTopics};
        my @places = (
            $space, $webid, $topicNID, $version, $cUIDNID, $commentNID, $now
        );
        $store->{dbh}->do($sql, {}, @places);
        $fobid = $store->{dbh}->last_insert_id(undef,undef,undef,undef);
    }
    $store->{FOBinfo}{$fobid} = {
        namespace => $space,
        webid => $webid,
        date => $now,
        NID => $topicNID,
        webName => $web,
        author => $cUID,
        topicName => $topic,
        version => $version,
        comment => $options->{comment},
    };

    # Now we prepare the values that need to be stored. We collect into
    # an array so we can call the DB once to store all the values
    my @values_text = (); # The placeholders required by SQL
    my @values_double = (); # The placeholders required by SQL
    my @values_datetime = (); # The placeholders required by SQL

    my %ducks = (
        strBit => 1, numBit => 2, dateBit => 4,
        nameseq_str => 0, string_only => 1,
        num_str => 3, date_str => 5, num_date_str => 7 );

    # For save we create a new rev. Therefore as latest
    # For reprev we overwrite the lastest rev so it's still latest
    # Only during upload with an option will we insert as other ver
    # because we are copying the history
    my $otherVersion = (defined $options->{_latest} && !$options->{_latest})
            ? $otherVersionBit : 0;

    push @values_text, $webid, $fobid, $otherVersion + $ducks{string_only},
        $textFID, "$meta->{_text}$EoV" if $meta->{_text};

    my $collectNames = sub {
        my ($type, $name, $nameSeq) = @_;
        $nameSeq = "$name000$EoV"
            if $Foswiki::Meta::VALIDATE{substr($type,0,-1)}{attachFOB};

        my ($FID, $fType) =
            @{$store->{FID}{2}{$type}{$nameSeq}{"name$EoV"}};
        push @values_text, $webid, $fobid, $otherVersion, $FID, "$name" if defined $name;
    };
    my $collectMeta = sub {
        my ($type, $name, $key, $valu) = @_;
        return if $Foswiki::Meta::VALIDATE{substr($type,0,-1)}{attachFOB};
    
        my ($FID, $fType) =
            defined $name ? @{$store->{FID}{1}{$type}{$name}{$key}}
                          : @{$store->{FID}{0}{$type}{$EoV}{$key}};

        if($fType == 1 || $fType == 2) {
            my $duck = $ducks{strBit};
            my $epoch;
            if($valu =~ /$store->{Number}/) {
                if($fType == 2) { # Epoch dates as string, num and date!
                    $duck = $ducks{num_date_str};
                    $epoch = $valu;
                }
                else {
                    $duck |= $ducks{numBit};
                }
            }
            else { # Technically not 100% compatible, numbers can be
                   # seen by parseTime as a date. Remove 'else' for 100%
                $epoch = Foswiki::Time::parseTime(substr($valu,0,70));
                $duck |= $ducks{dateBit} if $epoch;
            }
            $duck += $otherVersion;
            
            push @values_text, $webid, $fobid, $duck, $FID, "$valu$EoV";
            push @values_double, $webid, $fobid, $duck, $FID, $valu
                if $duck == $ducks{num_str} || $duck == $ducks{num_date_str};

            if($duck == $ducks{date_str} || $duck == $ducks{num_date_str}) {
                my $date = Foswiki::Time::formatTime($epoch, '$year-$mo-$day $hours:$minutes:$seconds');
                push @values_datetime, $webid, $fobid, $duck, $FID, $date;
            }
        }
    };
    _forEachField($meta, $collectNames, $collectMeta );

    if($options->{_repRev}) {    
        my $sql = 'delete from metaText where webid = ? and fobid = ?';
        my $sth = $store->{dbh}->do( $sql, {}, $webid, $fobid );

        $sql = 'delete from values_text where webid = ? and fobid = ?';
        $sth = $store->{dbh}->do( $sql, {}, $webid, $fobid );

        $sql = 'delete from values_double where webid = ? and fobid = ?';
        $sth = $store->{dbh}->do( $sql, {}, $webid, $fobid );

        $sql = 'delete from values_datetime where webid = ? and fobid = ?';
        $sth = $store->{dbh}->do( $sql, {}, $webid, $fobid );

        $sql = 'delete from access where webid = ? and fobid = ?';
        $sth = $store->{dbh}->do( $sql, {}, $webid, $fobid );
    }
    elsif($version > 1 && !$options->{_version}) {
        my $sql = "update FOBinfo " .
            "where webid = ? and fobid = ? " .
            "set namespace = $namespace{otherTopics}";
        $store->{dbh}->do($sql, {}, $webid, $fobid);

        $sql = "update metaText " .
            "where webid = ? and fobid = ? " .
            "set ducktype = $otherVersionBit";
        $store->{dbh}->do($sql, {}, $webid, $fobid);

        $sql = "update values_text " .
            "where webid = ? and fobid = ? " .
            "set ducktype = ducktype + $otherVersionBit";
        $store->{dbh}->do($sql, {}, $webid, $fobid);

        $sql = "update values_double " .
            "where webid = ? and fobid = ? " .
            "set ducktype = $otherVersionBit";
        $store->{dbh}->do($sql, {}, $webid, $fobid);

        $sql = "update values_datetime " .
            "where webid= ? and fobid = ? " .
            "set ducktype = $otherVersionBit";
        $store->{dbh}->do($sql, {}, $webid, $fobid);
    }

    {
        my @mText = split( /\n/, $metaText );
        my @mValues;
        for my $lnum (0..$#mText) {
            push @mValues, $webid, $fobid, $otherVersion, $lnum, "$mText[$lnum]\n"; # Returning \n from split(), $EoV is not appropriate
        }
        my $sql = 'insert into metaText ' .
                  '(webid, fobid, ducktype, lnum, value) values ' .
                 ('(?, ?, ?, ?, ?), ' x (scalar @mValues / 5) );
        $sql = substr($sql,0,-2);
        my $sth = $store->{dbh}->do( $sql, {}, @mValues ) if @mValues;
    }

    if(@values_text) {
        my $sql = 'insert into values_text ' .
                  '(webid, fobid, ducktype, FID, value) values' .
                  ('(?, ?, ?, ?, ?), ' x (scalar @values_text / 5));
        $sql = substr($sql,0,-2);
        my $sth = $store->{dbh}->do( $sql, {}, @values_text );
    }
    if(@values_double) {
        my $sql = 'insert into values_double ' .
                  '(webid, fobid, ducktype, FID, value) values' .
                  ('(?, ?, ?, ?, ?), ' x (scalar @values_double / 5));
        $sql = substr($sql,0,-2);
        my $sth = $store->{dbh}->do( $sql, {}, @values_double );
    }
    if(@values_datetime) {
        my $sql = 'insert into values_datetime ' .
                  '(webid, fobid, ducktype, FID, value) values' .
                  ('(?, ?, ?, ?, ?), ' x (scalar @values_datetime / 5));
        $sql = substr($sql,0,-2);
        my $sth = $store->{dbh}->do( $sql, {}, @values_datetime );
    }
    if(!$otherVersion && @{$rules}) {
        my @places;
        @places = map { (
            $webid, $fobid, $topicNID, $store->{NID}{$_->[0]}, $_->[1], $_->[2], $_->[3],
        ); } @{$rules};
        my $sql = "insert into access " .
            "(webId, fobid, topicNID, accessNID, permission, context, mode) values" .
            (substr('(?, ?, ?, ?, ?, ?, ?), ' x (scalar @places / 7), 0, -2));

        #print "======================\n$sql\n@places\n=====================\n";
        my $sth = $store->{dbh}->do( $sql, {}, @places );
    }

    $store->_commit;    

    return; #$rn    
}

=begin TML

---++ ObjectMethod repRev( $topicObject, $cUID, %options ) -> $rev
   * =$topicObject= - Foswiki::Meta topic object
Replace last (top) revision of a topic with different content. The different
content is taken from the content currently loaded in $topicObject.

Parameters and return value as saveTopic, except
   * =%options= - as for saveTopic, with the extra options:
      * =operation= - set to the name of the operation performing the save.
        This is used only in the log, and is normally =cmd= or =save=. It
        defaults to =save=.

Used to try to avoid the deposition of 'unecessary' revisions, for example
where a user quickly goes back and fixes a spelling error.

Also provided as a means for administrators to rewrite history (forcedate).

It is up to the store implementation if this is different
to a normal save or not.

Returns the id of the latest revision.

Implementation must invoke 'update' on event listeners.

=cut

sub repRev {
    my ( $this, $meta, $cUID, %options ) = @_;
    $options{_repRev} = 1;
    return $this->saveTopic($meta, $cUID, \%options);
}

=begin TML

---++ ObjectMethod delRev( $topicObject, $cUID ) -> $rev
   * =$topicObject= - Foswiki::Meta topic object
   * =$cUID= - cUID of user doing the deleting

Parameters and return value as saveTopic.

Provided as a means for administrators to rewrite history.

Delete last entry in repository, restoring the previous
revision.

It is up to the store implementation whether this actually
does delete a revision or not; some implementations will
simply promote the previous revision up to the head.

Implementation must invoke 'update' on event listeners.

=cut

sub delRev {
    my ( $this, $meta, $cUID ) = @_;

    my ($webName, $topicName) = ($meta->web, $meta->topic);
    my $ti = $this->_topicInfo($webName, $topicName);
    
    # SPECS: Will the store ever pass an object with latest ver=1 to delRev?
    # SPECS: Or for that matter a non existent topic?
    # SPECS: If so, this is just a failsafe, if not then should an
    # SPECS: exception be thrown instead?
    if(!$ti || $ti->{version} == 1) {
        die "delRev failed $webName.$topicName not found or version = 1";
    }
    
    my $sql =
        "select " .
            "fobid, namespace, webid, topic.NID as NID, version, " .
            "date as datestr, unix_timestamp(date) as date, " .
            "author as authorNID, author.name as author, " .
            "comment as commentNID, comment.name as comment, " .
            "reprev " . 
        "from FOBinfo topic, names comment, names author " .
        "where webid = " .
        "(select fobid from FOBinfo " . 
            "where namespace = $namespace{latestWebs} " .
            "and webid = $rootId " .
            "and NID = (select NID from names where name = ?)" .
        ") " .
        "and topic.NID = (select NID from names where name = ?) " .
        "and author = author.NID and comment = comment.NID " .
        "and namespace = $namespace{otherTopics} " .
        "and version = " . 
        "(select max(version) " .
            "from FOBinfo topic " .
            "where webid = " .
            "(select fobid from FOBinfo " . 
                "where namespace = $namespace{latestWebs} " .
                "and webid = $rootId " .
                "and NID = (select NID from names where name = ?)" .
            ") " .
            "and namespace = $namespace{otherTopics} " .
            "and topic.NID = (select NID from names where name = ?)" .
        ")";
    my $sth = $this->{dbh}->prepare_cached($sql);
    my @places = ("$webName$EoV", "$topicName$EoV");
    $sth->execute(@places, @places); # Yes twice! repeated in subquery
    my $fobids = $sth->fetchall_arrayref({});

    if(!@{$fobids}) {
        die "delRev failed $webName.$topicName no prior version";
    }
    # Former version info successfully retrieved, cache the details
    my $fi = $fobids->[0];
    $this->_cacheName($fi->{NID}, "$topicName$EoV");
    $this->_cacheName($fi->{authorNID}, $fi->{author});
    $this->_cacheName($fi->{commentNID}, $fi->{comment});
    chop($fi->{author}, $fi->{comment});

    $fi->{webName} = $webName;
    $fi->{topicName} = $topicName;

    my $user = $fi->{author}
        || $Foswiki::Users::BaseUserMapping::UNKNOWN_USER_CUID;
    $fi->{user} = $user;

    $this->{topicInfo}{$webName}{$topicName}{$fi->{version}} = $fi;
    $this->{FOBinfo}{$fi->{fobid}} = $fi;

    # We now have $ti as the latest or *t*op info
    # and $fi as the prior or *f*ormer info
    # So we need to delete all refs to $ti->{fobid} and update
    # $fi->{fobid} references where applicable to mark as latest again

    $sth = $this->_commit(); # Effectively start following as transaction
    
    $sql = 'delete from metaText where webid = ? and fobid = ?';
    $sth = $this->{dbh}->do( $sql, {}, $ti->{webid}, $ti->{fobid} );

    $sql = 'delete from values_text where webid = ? and fobid = ?';
    $sth = $this->{dbh}->do( $sql, {}, $ti->{webid}, $ti->{fobid} );

    $sql = 'delete from values_double where webid = ? and fobid = ?';
    $sth = $this->{dbh}->do( $sql, {}, $ti->{webid}, $ti->{fobid} );

    $sql = 'delete from values_datetime where webid = ? and fobid = ?';
    $sth = $this->{dbh}->do( $sql, {}, $ti->{webid}, $ti->{fobid} );

    $sql = 'delete from FOBinfo where fobid = ?';
    $sth = $this->{dbh}->do( $sql, {}, $ti->{fobid} );

    $sql = "update FOBinfo where fobid = ? " .
        "set namespace = $namespace{latestTopics}";
    $this->{dbh}->do($sql, {}, $fi->{fobid});

    $sql = "update values_text where webid = ? and fobid = ? " .
        "set ducktype = ducktype - $otherVersionBit";
    $this->{dbh}->do($sql, {}, $fi->{fobid}, $fi->{fobid});

    $sql = "update values_double where webid = ? and fobid = ? " .
        "set ducktype = 0";
    $this->{dbh}->do($sql, {}, $fi->{webid}, $fi->{fobid});

    $sql = "update values_datetime where webid = ? and fobid = ? " .
        "set ducktype = 0";
    $this->{dbh}->do($sql, {}, $fi->{webid}, $fi->{fobid});
    
    $this->_commit();

    # reload the topic object
    $meta->unload();
    $meta->loadVersion();

    return ($fi->{version}, 1);
}

=begin TML

---++ ObjectMethod atomicLockInfo( $topicObject ) -> ($cUID, $time)
If there is a lock on the topic, return it.

=cut

sub atomicLockInfo {
    my ( $this, $meta ) = @_;
    
    my ($webName, $topicName) = ($meta->web, $meta->topic);
    $webName =~ s#\.#/#go;
    
    my $sql =
        "select cUIDt.name as cUID, unix_timestamp(time_dt) as time " .
        "from locks, names cUIDt " .
        "where webNID = (select NID from names where name = ?) " .
        "and topicNID = (select NID from names where name = ?) " .
        "and  cUIDNID = cUIDt.NID";
    my $sth = $this->{dbh}->prepare_cached($sql);
    $sth->execute("$webName$EoV", "$topicName$EoV");
    my $locks = $sth->fetchall_arrayref( {} );
    
    if(@{$locks}) {
        # Should match only one lock, so we only return 1st one
        my $lock = $locks->[0];
        return ($lock->{cUID}, $lock->{time});
    }

    return ( undef, undef );
}

=begin TML

---++ ObjectMethod atomicLock( $topicObject, $cUID )

   * =$topicObject= - Foswiki::Meta topic object - $web, $topic address only (Lock 'whole' topic)
   * =$cUID= cUID of user doing the locking
Grab a topic lock on the given topic.

=cut

sub atomicLock {
    my ( $this, $meta, $cUID ) = @_;
    my ($webName, $topicName) = ($meta->web, $meta->topic);
    $webName =~ s#\.#/#go;
    
    $this->{missing} = {};
    $this->_addNames("$webName$EoV", "$topicName$EoV", "$cUID$EoV");
    $this->_readNames();
    $this->_commit(); # Logical start transaction
    $this->_insertNames();
    $this->_readNames();
    
    my $sql = "insert into locks ".
        "(webNID, topicNID, cUIDNID, time_dt) values (?,?,?,now()) " .
        "on duplicate key update " .
        "cUIDNID = ?, time_dt = now()";
    $this->{dbh}->do($sql, {},
        $this->{NID}{"$webName$EoV"},
        $this->{NID}{"$topicName$EoV"},
        $this->{NID}{"$cUID$EoV"},
        $this->{NID}{"$cUID$EoV"} # Repeasted in case updating same lock
    );
    $this->_commit();
}

=begin TML

---++ ObjectMethod atomicUnlock( $topicObject )

   * =$topicObject= - Foswiki::Meta topic object - WTAddr lock whole
Release the topic lock on the given topic. A topic lock will cause other
processes that also try to claim a lock to block. It is important to
release a topic lock after a guard section is complete. This should
normally be done in a 'finally' block. See man Error for more info.

Topic locks are used to make store operations atomic. They are
_note_ the locks used when a topic is edited; those are Leases
(see =getLease=)

=cut

sub atomicUnlock {
    my ( $this, $meta ) = @_;
    my ($webName, $topicName) = ($meta->web, $meta->topic);
    $webName =~ s#\.#/#go;
    
    $this->_commit();
    my $sql =
        "delete from locks " .
        "where webNID = (select NID from names where name = ?) " .
        "and topicNID = (select NID from names where name = ?) ";
    $this->{dbh}->do($sql, {}, "$webName$EoV", "$topicName$EoV");
    $this->_commit();
}

=begin TML

---++ ObjectMethod webExists( $web ) -> $boolean

Test if web exists
   * =$web= - Web name, required, e.g. ='Sandbox'=

=cut

sub webExists {
    my ( $this, $web ) = @_;
    return defined
        $this->_topicInfo( $web, $this->{cfg}{Versatile}{WebPrefsTopicName} );
}

=begin TML

---++ ObjectMethod topicExists( $web, $topic ) -> $boolean

Test if topic exists
   * =$web= - Web name, optional, e.g. ='Main'=
   * =$topic= - Topic name, required, e.g. ='TokyoOffice'=, or ="Main.TokyoOffice"=

=cut

# Hmmm... I note that PlainFile expects a web! Spec above say 'optional'

sub topicExists {
    my ( $this, $web, $topic ) = @_;
    return defined $this->_topicInfo($web, $topic);
}

=begin TML

---++ ObjectMethod getApproxRevTime (  $web, $topic  ) -> $epochSecs

Get an approximate rev time for the latest rev of the topic. This method
is used to optimise searching. Needs to be as fast as possible.

=cut

sub getApproxRevTime {
    my ( $this, $web, $topic ) = @_;
    my $ti = $this->_topicInfo($web, $topic);
    return $ti->{date} if $ti;
    return 0;
}

=begin TML

---++ ObjectMethod eachChange( $web, $time ) -> $iterator

Get an iterator over the list of all the changes in the given web between
=$time= and now. $time is a time in seconds since 1st Jan 1970, and is not
guaranteed to return any changes that occurred before (now -
{Store}{RememberChangesFor}). Changes are returned in most-recent-first
order.

=cut

#sub eachChange {
#    my ( $this, $meta, $since ) = @_;
#}

=begin TML

---++ ObjectMethod recordChange(%args)
Record that the store item changed, and who changed it

This is a private method to be called only from the store internals, but it can be used by 
$Foswiki::Cfg{Store}{ImplementationClasses} to chain in to eveavesdrop on Store events

        cuid          => $cUID,
        revision      => $rev,
        verb          => $verb,
        newmeta       => $topicObject,
        newattachment => $name

=cut

#sub recordChange {
#    my $this = shift;
#    my %args = ( 'more', '', @_ );
#
#    $this->{missing} = {};
#     $this->_addNames(
#
#    my $file = _getData( $args{_meta}->web ) . '/.changes';
#    my @changes;
#    my $text = '';
#    my $t    = time;
#
#    if ( -e $file ) {
#        my $cutoff = $t - $this->{cfg}{Versatile}{RememberChangesFor};
#        my $fh;
#        open( $fh, '<', $file )
#          or die "PlainFile: failed to read $file: $!";
#        local $/ = "\n";
#        my $head = 1;
#        while ( my $line = <$fh> ) {
#            chomp($line);
#            if ($head) {
#                my @row = split( /\t/, $line, 4 );
#                next if ( $row[2] < $cutoff );
#                $head = 0;
#            }
#            $text .= "$line\n";
#        }
#        close($fh);
#    }
#
#     Add the new change to the end of the file
#    $text .= $args{_meta}->topic || '.';
#    $text .= "\t$args{cuid}\t$t\t$args{revision}\t$args{more}\n";
#
#    _saveFile( $file, $text );
#}

=begin TML

---++ ObjectMethod eachAttachment( $topicObject ) -> \$iterator

Return an iterator over the list of attachments stored for the given
topic. This will get a list of the attachments actually stored for the
topic, which may be a longer list than the list that comes from the
topic meta-data, which only lists the attachments that are normally
visible to the user.

The iterator iterates over attachment names.

The $topicObject passed can come from a Plugin author, so do not make assumptions. The only thing you can assume is to treat the web and topic of the meta are the identity of the topic to retrieve.

Called from Foswiki::Meta and Foswiki::Func (a sub stub eachAttachment is found in Store.pm, other Stores will implement this method).

Foswiki::Func creates a fresh new unloaded $meta, populates with $web and $topic from user, then calls the store eachAttachment.
Foswiki::Meta passes straight thru, any rubbish is possible.

Foswiki only has one 'current' set of attachments which is what we return here (no $rev passed anyway). Versatile (and other stores) do keep historic attachment meta but it's not possible (necessarily) to retrieve an old attachment. To be clear, each attachment does have revisions, but for example if an attachment is deleted then the attachment and all it's revisions are gone. On old version of the topic may well contain meta of an old attachment revision, but the physical document is no longer stored.

=cut

=begin TML

---++ ObjectMethod eachTopic( $webObject ) -> $iterator

Get list of all topics in a web as an iterator

=cut

sub eachTopic {
    my ( $this, $meta) = @_;

    my $webName = $meta && $meta->web;
    $webName = '' if !defined $webName;
    $webName =~ s#\.#/#go;

    my $sql =
        "select " .
            "topic.name as topicName " .
        "from FOBinfo t, names topic " .
        "where webid = " .
        "(select fobid from FOBinfo " . 
            "where namespace = $namespace{latestWebs} " .
            "and webid = $rootId " .
            "and NID = (select NID from names where name = ?)" .
        ") " .
        "and t.NID = topic.NID " .
        "and namespace = $namespace{latestTopics} " .
        "order by topicName";
 
    my $sth = $this->{dbh}->prepare($sql);
    $sth->execute("$webName$EoV");
    my $rows = $sth->fetchall_arrayref( {} );

    my @list = map {
        my $fi = $_;
        chop($fi->{topicName});
        $fi->{topicName};
    } @{$rows};
    
    require Foswiki::ListIterator;
#    return new Foswiki::ListIterator( $rows );
    return new Foswiki::ListIterator( \@list );
}

sub eachTopic2 {
    my ( $this, $meta) = @_;

    my $webName = $meta && $meta->web;
    $webName = '' if !defined $webName;
    $webName =~ s#\.#/#go;

    my $sql =
        "select " .
            "fobid, namespace, webid, t.NID as NID, version, " .
            "date as datestr, unix_timestamp(date) as date, " .
            "author as authorNID, author.name as author, " .
            "comment as commentNID, comment.name as comment, " .
            "topic.name as topicName, " .
            "reprev " . 
        "from FOBinfo t, names comment, names author, names topic " .
        "where webid = " .
        "(select fobid from FOBinfo " . 
            "where namespace = $namespace{latestWebs} " .
            "and webid = $rootId " .
            "and NID = (select NID from names where name = ?)" .
        ") " .
        "and t.NID = topic.NID " .
        "and author = author.NID and comment = comment.NID " .
        "and namespace = $namespace{latestTopics} " .
        "order by topicName";
 
    my $sth = $this->{dbh}->prepare($sql);
    $sth->execute("$webName$EoV");
    my $rows = $sth->fetchall_arrayref( {} );

    my @list = map {
        my $fi = $_;
        $this->_cacheName($fi->{NID}, $fi->{topicName});
        $this->_cacheName($fi->{authorNID}, $fi->{author});
        $this->_cacheName($fi->{commentNID}, $fi->{comment});
        chop($fi->{topicName}, $fi->{author}, $fi->{comment});

        $fi->{webName} = $webName;
        my $user = $fi->{author}
            || $Foswiki::Users::BaseUserMapping::UNKNOWN_USER_CUID;
        $fi->{user} = $user;

        $this->{topicInfo}{$webName}{$fi->{topicName}}{$fi->{version}} = $fi;
        $this->{topicInfo}{$webName}{$fi->{topicName}}{0} = $fi;
        $this->{FOBinfo}{$fi->{fobid}} = $fi;

        $fi->{topicName};
    } @{$rows};
    
    require Foswiki::ListIterator;
    return new Foswiki::ListIterator( \@list );
}

=begin TML

---++ ObjectMethod eachWeb($webObject, $all ) -> $iterator

Return an iterator over each subweb. If $all is set, will return a list of all
web names *under* $web. The iterator returns web pathnames relative to $web.

The list of web names is sorted alphabetically by full path name e.g.
   * AWeb
   * AWeb/SubWeb
   * AWeb/XWeb
   * BWeb

If the webObject is undef (the root object) then return all top level webs   

=cut

sub eachWeb {
    my ( $this, $meta, $all ) = @_;

    my $web = $meta && $meta->web;
    $web = '' if !defined $web;
    $web =~ s#\.#/#go;

    my $sql = "select webname.name, t.webid, w.nid " . 
        "from FOBinfo t, FOBinfo w, names webname " .
        "where t.namespace = 0 " .
        "and t.NID " . 
        "= (select NID from names where name = ?) " .
        "and t.webid = w.fobid and w.NID = webname.NID " .
        (!$meta && $all
            ? ""
            : "and w.NID in (select NID from names where name regexp ?) ") .
        "order by webname.name";
    my $sth = $this->{dbh}->prepare($sql);
    my @places = ( "$this->{cfg}{Versatile}{WebPrefsTopicName}$EoV" );

    $web .= '/' if $web ne '';
    push @places, ($all ? "^" . $web . ".*\$" : "^" . $web . "[^/]*\$")
        if ($meta || !$all);
    $sth->execute(@places);
    my $rows = $sth->fetchall_arrayref();
    
    my @list = map {
        $this->{webId}{$_->[0]} = $_->[1];
        $this->{webName}{$_->[1]} = $_->[0];
        $this->{name}{$_->[2]} = "$_->[0]$EoV";
        $this->{NID}{"$_->[0]$EoV"} = $_->[2];
        substr($_->[0],0,-1);
    } @{$rows};
    
    require Foswiki::ListIterator;
    return new Foswiki::ListIterator( \@list );
}

=begin TML

---++ ObjectMethod remove( $cUID, $om, $attachment )
   * =$cUID= who is doing the removing
   * =$om= - thing being removed (web or topic)
   * =$attachment= - optional attachment being removed

Destroy a thing, utterly.

=cut

sub remove {
    my ( $this, $cUID, $meta, $attachment ) = @_;

    if($attachment) {
        $this->SUPER::remove($cUID, $meta, $attachment);
        return;
    }

    my ($webName, $topicName) = ($meta->web, $meta->topic);
    $webName =~ s#\.#/#go;
    
    my $webTopicSelect = 
        "select fobid from FOBinfo where " .
            "namespace in " .
                "($namespace{latestTopics}, " .
                 "$namespace{otherTopics}), " . 
            "and webid = (select fobid from FOBinfo " . 
                "where namespace = $namespace{latestWebs} " .
                "and webid = $rootId " .
                "and NID = (select NID from names where name = ?)" .
            ")";
    my @places = ("$webName$EoV");

    if($topicName) {
        $webTopicSelect .=
            " and topic.NID = (select NID from names where name = ?)";
        push @places, "$topicName$EoV";
    }

    # Need to delete in the right order because of ref-integ
    
    $this->{dbh}->do("delete from values_text where fobid in " .
        "(select fobid from FOBinfo where $webTopicSelect)",
        {}, @places);
    $this->{dbh}->do("delete from values_double where fobid in " .
        "(select fobid from FOBinfo where $webTopicSelect)",
        {}, @places);
    $this->{dbh}->do("delete from values_datetime where fobid in " .
        "(select fobid from FOBinfo where $webTopicSelect)",
        {}, @places);
        
    $this->{dbh}->do("delete from FOBinfo " .
        "where $webTopicSelect", {}, @places);
        
    $this->_commit();
}

=begin TML

---++ ObjectMethod query($query, $inputTopicSet, $session, \%options) -> $outputTopicSet

Search for data in the store (not web based).
   * =$query= either a =Foswiki::Search::Node= or a =Foswiki::Query::Node=.
   * =$inputTopicSet= is a reference to an iterator containing a list
     of topic in this web, if set to undef, the search/query algo will
     create a new iterator using eachTopic() 
     and the topic and excludetopics options

Returns a =Foswiki::Search::InfoCache= iterator

This will become a 'query engine' factory that will allow us to plug in
different query 'types' (Sven has code for 'tag' and 'attachment' waiting
for this)

=cut

#sub query {
#    my ( $this, $query, $inputTopicSet, $session, $options ) = @_;
#
#    my $engine;
#    if ( $query->isa('Foswiki::Query::Node') ) {
#        unless ( $this->{queryObj} ) {
#            my $module = $this->{cfg}{Versatile}{QueryAlgorithm};
#            eval "require $module";
#            die
#"Bad {Store}{QueryAlgorithm}; suggest you run configure and select a different algorithm\n$@"
#              if $@;
#            $this->{queryObj} = $module->new();
#        }
#        $engine = $this->{queryObj};
#    }
#    else {
#        ASSERT( $query->isa('Foswiki::Search::Node') ) if DEBUG;
#        unless ( $this->{searchQueryObj} ) {
#            my $module = $this->{cfg}{Versatile}{Searchlgorithm};
#            eval "require $module";
#            die
#"Bad {Store}{SearchAlgorithm}; suggest you run configure and select a different algorithm\n$@"
#              if $@;
#            $this->{searchQueryObj} = $module->new();
#        }
#        $engine = $this->{searchQueryObj};
#    }
#
#    no strict 'refs';
#    return $engine->query( $query, $inputTopicSet, $session, $options );
#    use strict 'refs';
#}

=begin TML

---++ ObjectMethod getRevisionAtTime( $topicObject, $time ) -> $rev

   * =$topicObject= - topic
   * =$time= - time (in epoch secs) for the rev

Get the revision identifier of a topic at a specific time.
Returns a single-digit rev number or undef if it couldn't be determined
(either because the topic isn't that old, or there was a problem)

=cut

sub getRevisionAtTime {
    my ( $this, $meta, $time ) = @_;
    my ($webName, $topicName) = ($meta->web, $meta->topic);

    return undef unless defined $webName && $webName ne '';
    return undef unless defined $topicName && $topicName ne '';

    $webName =~ s#\.#/#go;
    
    my $sql =
        "select " .
            "fobid, namespace, webid, topic.NID as NID, version, " .
            "date as datestr, unix_timestamp(date) as date, " .
            "author as authorNID, author.name as author, " .
            "comment as commentNID, comment.name as comment, " .
            "reprev " . 
        "from FOBinfo topic, names comment, names author " .
        "where webid = " .
        "(select fobid from FOBinfo " . 
            "where namespace = $namespace{latestWebs} " .
            "and webid = $rootId " .
            "and NID = (select NID from names where name = ?)" .
        ") " .
        "and topic.NID = (select NID from names where name = ?) " .
        "and author = author.NID and comment = comment.NID " .
        "and namespace " .
             "in ($namespace{latestTopics}, $namespace{otherTopics}) " .
        "and date <= unix_timestamp(?) order by date desc limit 1";

    my $sth = $this->{dbh}->prepare_cached($sql);
    $sth->execute("$webName$EoV", "$topicName$EoV", $time);
    my $fobids = $sth->fetchall_arrayref({});

    if(@{$fobids}) {
        my $fi = $fobids->[0];
        $this->_cacheName($fi->{NID}, "$topicName$EoV");
        $this->_cacheName($fi->{authorNID}, $fi->{author});
        $this->_cacheName($fi->{commentNID}, $fi->{comment});
        chop($fi->{author}, $fi->{comment});

        $fi->{webName} = $webName;
        $fi->{topicName} = $topicName;

        my $user = $fi->{author}
            || $Foswiki::Users::BaseUserMapping::UNKNOWN_USER_CUID;
        $fi->{user} = $user;

        $this->{topicInfo}{$webName}{$topicName}{$fi->{version}} = $fi;
        $this->{topicInfo}{$webName}{$topicName}{0} = $fi
            if $fi->{namespace} == $namespace{latestTopics};
        $this->{FOBinfo}{$fi->{fobid}} = $fi;

        return $fi->{version};
    }
    return undef;
}

=begin TML

---++ ObjectMethod getLease( $topicObject ) -> $lease

   * =$topicObject= - topic

If there is a lease on the topic, return the lease, otherwise undef.
A lease is a block of meta-information about a topic that can be
recovered (this is a hash containing =user=, =taken= and =expires=).
Leases are taken out when a topic is edited. Only one lease
can be active on a topic at a time. Leases are used to warn if
another user is already editing a topic.

In FW 1.0.10 I was able to rename a topic that had a lease without any warning. In addition the lease
file was deleted, so the edit lost its lease.

I deliberately tested renaming while editing a topic.

I saw two issues:
   1 The rename was allowed (no checking of the lease)
   2 The lease was also deleted
      * So the ongoing edit was not protected any more as well

At the very least this is unexpected behaviour. A rename is a form of topic edit and should respect the lease should it not?

According to f.o the docs clearly say that rename should test for this. It could be I'm behind the curve, need to test on 1.1.7 at some point.
=cut

# select hash from lease where webNameId = ? and topicNameId = ?"
# select lease from FOBinfo where webNameId = ? and topicNameId = ?"
sub getLease {
    my ($this, $meta) = @_;

    my ($webName, $topicName) = ($meta->web, $meta->topic);
    $webName =~ s#\.#/#go;
    
    my $sql =
        "select usert.name as user, " .
        "unix_timestamp(taken_dt) as taken, " .
        "unix_timestamp(expires_dt) as expires " .
        "from lease, names usert " .
        "where webNID = (select NID from names where name = ?) " .
        "and topicNID = (select NID from names where name = ?) " .
        "and  userNID = usert.NID";
    my $sth = $this->{dbh}->prepare_cached($sql);
    $sth->execute("$webName$EoV", "$topicName$EoV");
    my $leases = $sth->fetchall_arrayref( {} );
    
    # Should match only one lease (unique key), so we only return 1st one
    return $leases->[0] if @{$leases};
        
    return undef;    
}

=begin TML

---++ ObjectMethod setLease( $topicObject, $lease )

   * =$topicObject= - Foswiki::Meta topic object
   * =$lease= - ref to hash with 3 keys: taken, user, expires
Take out (taken) a lease on the given topic for this user until expires.

See =getLease= for more details about Leases.

=cut

sub setLease {
    my ( $this, $meta, $lease ) = @_;

    my ($webName, $topicName) = ($meta->web, $meta->topic);
    $webName =~ s#\.#/#go;

    $this->_commit(); # Logical start transaction
    if($lease) {
        $this->{missing} = {};
        $this->_addNames("$webName$EoV", "$topicName$EoV",
            "$lease->{user}$EoV");
        $this->_readNames();
        $this->_insertNames();
        $this->_readNames();
        
        # on duplicate key update to insert or update the lease
        my $sql = "insert into lease ".
            "(webNID, topicNID, userNID, taken_dt, expires_dt) ".
            "values (?,?,?,from_unixtime(?),from_unixtime(?)) " .
            "on duplicate key update " .
            "userNID = ?, " . 
            "taken_dt = from_unixtime(?), " .
            "expires_dt = from_unixtime(?)";
        $this->{dbh}->do($sql, {},
            $this->{NID}{"$webName$EoV"},
            $this->{NID}{"$topicName$EoV"},
            $this->{NID}{"$lease->{user}$EoV"},
            $lease->{taken},
            $lease->{expires},
            $this->{NID}{"$lease->{user}$EoV"},
            $lease->{taken},
            $lease->{expires}
        );
    }
    else {
        my $sql =
            "delete from lease " .
            "where webNID = (select NID from names where name = ?) " .
            "and topicNID = (select NID from names where name = ?) ";
        $this->{dbh}->do($sql, {}, "$webName$EoV", "$topicName$EoV");
    }
    $this->_commit();
}

=begin TML

---++ ObjectMethod removeSpuriousLeases( $web )

Remove leases that are not related to a topic. These can get left behind in some store implementations when a topic is created, but never saved.

I don't think that's right. FW will call setLease when a new topic starts to be edited. Therefore a lease is created even if no topic is created. Its possible that some stores might also create spurious *topics* when setLease is called. Versatile needs to create a unique fobId which is not worth reclaiming, it's simply marked dangling on creation and changed to non-dangling if the save actually occurs.

That implies that some store could be implemented to never leave behind a spurious lease, but I do not see how. IIUC then a sp_lease is created whenever an edit session starts for a new topic at which point FW must ask the store to set a lease. This is required to ensure that an attempt to create a topic with the same name in another session will be challenged. How can a store return this information unless it's (ahem) stored. If the user obliges by saving the topic or pressing cancel then the lease is cleared (set to undef via setLease). If on the other hand the user clicks the back button or kills their browser (or it crashes etc, and we cannot circumvent this), then the lease is left parked in the store, but to a topic that is no more. It has become spurious.

Basically, any store will need to scan for leases which do not have a matching topic with that name.

This task is necessarily done during down-time, otherwise the above could remove leases that are still part of the active edit of a new topic.

I wondered about creating a new-topic lease table in memory. That way when the DB server dies it will implicitly take away any spurious leases. However, it will also take away 'spurious' leases at this time that are still active. It should be possible to bring back the DB server quickly, and users still able to save any work - or indeed carry on without any interruption.

Finally, it seems to me that deleting any lease after it's expire date
is sufficient and arguably better. Old leases are still spurious after all.
=cut

sub removeSpuriousLeases {
    my ( $this, $web ) = @_;
    
    my $sth = $this->{dbh}->do(
        "delete from lease where expires_dt < now()");
}
#############################################################################
# PRIVATE FUNCTIONS
#############################################################################

# nameSpace, webName, TopicName, rev
# if namespace = currentTopics then $rev must be 0

# Note: No $rev    means get current fobId (-2, -3 etc *NOT* applicable)
#       $rev ==  0 means get dangling entry
#       $rev >=  1 Means reads specific revision (could be same as current) 

sub _insertField {
    # Only kept to let _insertValue to compile, when that's rewritten it will
    # not use this at all
}

sub _insertValue {
    my ($this, $fobId, $FID, $value, $baseWeb) = @_;

    my $ducktype = 4;
    my $epoch;
    my $baseWebId = $this->_insertWeb($baseWeb,0);

    $ducktype = 1 if $value && $value =~ /$this->{Number}/; # Number and string
    
    if($ducktype == 4 && $FID != $this->{fields}->{_text}->{1}->{''}->{''}) {
        $epoch = Foswiki::Time::parseTime(substr($value,0,100));
        $ducktype = 2 if $epoch;
    }
    my $v = $value;
    
    # NOTE: This following note is now an anachronism, because of sequencing the name will be a field in the vlaues table. Therefore, I expect that it will be scanned.
    # Note: The metamember ('name') of the field-id is not scanned as a possible ForeLink, but fields is a small table so just scan that instead

    my $tagEnd = '[{%]'; # Could not write regex to parse correctly without this

    my %count = ( verbatim => 0, literal => 0, pre => 0, noautolink => 0);
    ELEMENT:
    while($v =~
        m/
            (^\s*<(\/)?(?i:(verbatim|literal|pre|noautolink))\b[^>]*>\s*$) |
            (^(?:\t|\ \ \ )+\*\s+(Set|Local)\s+($Foswiki::regex{tagNameRegex})\s*=\s*) |
            (\[\[[^\n]*?(?:\]\[|\]\])) |
            ((%MAINWEB%|%SYSTEMWEB%|%USERSWEB%|%WEB%|$Foswiki::regex{webNameRegex})(\.$Foswiki::regex{topicNameRegex})) |
            (%($Foswiki::regex{tagNameRegex})$tagEnd) |
            ($Foswiki::regex{topicNameRegex}) 
        /xmsgo) {
        my ($start, $end, $block, $blockend, $blockstart, $setvar, $setlocal, $settag, $squab, $wtlink, $web, $wtopic, $tag, $tagName, $tlink) = ($-[0], $+[0], $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14);
        if($block) {
            if($blockend) {
                $count{$blockstart}--;
            }
            else {
                $count{$blockstart}++;
            }
            next ELEMENT;
        }
        my $bcounts = ($count{verbatim} > 0 ? '+verbatim':'').($count{literal} > 0 ? '+literal':'').($count{pre} > 0 ? '+pre':'').($count{noautolink} > 0 ? '+noautolink':'');
        if($setvar) {
            # The Set/Local pref we've captured was only to ignore it. The $settag will be noted elsewhere as a preference so it's already Xreffed in a manner of speaking
            # The value part of the Set/local is further scanned for various Xref items
            next ELEMENT;
        }
        if($tlink && $tlink !~ m{\A \s* \Z}msx )  {
            $this->_insertXref($this->_insertField('_forelink',$bcounts,''),
                               $fobId,$FID,
                               $this->_insertTopic($baseWebId,$tlink,1),0);
            next ELEMENT;
        }
        if($wtlink && $wtlink !~ m{\A \s* \Z}msx ) {
            $wtlink =~ s/%WEB%/$baseWeb/;
            my ($W, $T) = Foswiki::Func::normalizeWebTopicName('', $wtlink);
            $this->_insertXref($this->_insertField('_forelink',$bcounts,''),
                               $fobId, $FID,
                               $this->_insertTopic($this->_insertWeb($W,1), $T, 1), 0);
            next ELEMENT;
        }
        if($tag) {
#            $this->_insertXref($this->_insertField('_foreTag',$bcounts,''),
#                                           $fobId, $FID,
#                                           $this->_insertTopic($baseWebId, $tagName, 1), 0);
            next ELEMENT;
        }
        if($squab) {
            my ($sqlink) = $squab =~ m/\[\[([^\n]*?)(?:\]\[|\]\])/;
            if($sqlink =~ m/^([ $Foswiki::regex{mixedAlpha}$Foswiki::regex{numeric}]+)$/ && $sqlink !~ m{\A \s* \Z}msx ) {
                my $w2 = $sqlink;
                $w2 =~ s/\s*([$Foswiki::regex{mixedAlpha}$Foswiki::regex{numeric}]+)\s*/ucfirst($1)/ge;
                $this->_insertXref($this->_insertField('_forelink', $bcounts, ''),
                                   $fobId,$FID,
                                   $this->_insertTopic($baseWebId, $w2, 1), 0);
                next ELEMENT;
            }
            SQUAB_ELEMENT:
            while($sqlink =~ 
                    m/
                    ((%MAINWEB%|%SYSTEMWEB%|%USERSWEB%|%WEB%|$Foswiki::regex{webNameRegex})(\.$Foswiki::regex{topicNameRegex})) |
                    (%($Foswiki::regex{tagNameRegex})$tagEnd) |
                    ($Foswiki::regex{topicNameRegex}) 
                /xmsg) {
                    my ($start, $end, $wtlink, $web, $wtopic, $tag, , $tagName, $tlink) = ($-[0], $+[0], $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11);
                    if($tlink && $tlink !~ m{\A \s* \Z}msx ) {
                        $this->_insertXref($this->_insertField('_forelink',$bcounts,''),
                                           $fobId,$FID,
                                           $this->_insertTopic($baseWebId,$tlink,1),0);
                        next SQUAB_ELEMENT;
                    }
                    if( $wtlink && $wtlink !~ m{\A \s* \Z}msx ) {
                        $wtlink =~ s/%WEB%/$baseWeb/;
                        my ($W, $T) = Foswiki::Func::normalizeWebTopicName('', $wtlink);
                        $this->_insertXref($this->_insertField('_forelink',$bcounts,''),
                                           $fobId, $FID,
                                           $this->_insertTopic($this->_insertWeb($W,1), $T, 1), 0);
                        next SQUAB_ELEMENT;
                    }
                    if($tag) {
#                        $this->_insertXref($this->_insertField('_foreTag',$bcounts,''),
#                                           $fobId,$FID,
#                                           $this->_insertTopic($baseWebId,$tagName,1),0);
                        next SQUAB_ELEMENT;
                    }
            }
            next ELEMENT;
        }
    }

    $this->_insertFlush();    
}

sub _recreate {
    my ($dbh, $table, $opts) = @_;

    my $sth = $dbh->do("drop table if exists $table");

    my $engine = $opts->{engine};
    $engine = 'InnoDB' if !defined $engine;
    $engine = ", engine = $engine" if $engine;
    my $autoinc = $opts->{auto_increment};
    $autoinc = '' if !defined $autoinc;
    $autoinc = ", auto_increment = $autoinc" if $autoinc;

    $sth = $dbh->do(
        "create table $table " .
        "($opts->{columns}) " .
        "character set utf8mb4, " .
#        "character set utf8, " .
        "collate utf8mb4_bin$engine$autoinc"
#        "collate utf8_bin$engine$autoinc"
    );
    print "Created $table OK\n";
}

sub create {
    my $this = shift;  
    $this->SUPER::create(@_);
 
    my $dbh = $this->{dbh};
    print "Versatile::create $dbh\n";
    

    my $nameSize = $nameLen + 1; # Extra 1 for end of name marker see $EoV
    
    _recreate($dbh, 'names', {
        columns => 
            "name text($nameSize) not null, " .
            "NID $NIDtype not null auto_increment primary key, " .
            "unique index NID_by_name (name($nameSize))",
        auto_increment => $NIDbase
    } );

    
    _recreate($dbh, 'FOBinfo', {
        columns => 
            "fobid $fobIdType not null auto_increment primary key, " .
            "namespace tinyint not null, " .
            "webId $fobIdType not null, " .
            "NID $NIDtype not null, " .
            "version $versionType not null, " .
            "date datetime not null, " .
            "author $NIDtype not null, " .
            "comment $NIDtype not null, " .
            "reprev $versionType, " .
            "unique index fobId_by_webTopic " .
                "(namespace, webId, NID, version), " .
            "index fobId_by_revdate (date) " .
            ""
    } );
    
    my $blankNID = $persist->{NID}{$EoV}; # NID for ''
    my $sql = "insert into FOBinfo " .
        "(fobid, namespace, webid, NID, version, author, comment, date) " .
        "values (?, ?, ?, ?, ?, ?, ?, now())";
    $this->{dbh}->do($sql, {}, $rootId, $namespace{root},
        $rootId, $blankNID, 0, $blankNID, $blankNID);

    _recreate($this->{dbh}, 'fields', {
        columns => 
            "FID $FIDtype not null auto_increment primary key, " .
            "hasName tinyint NOT NULL, " .
            "typeNID $NIDtype NOT NULL, " .
            "nameNID $NIDtype NOT NULL, " .
            "keyNID $NIDtype NOT NULL, " .
            "fieldType tinyint NOT NULL default 1, " .
            "unique index FID_by_field (hasName, typeNID, nameNID, keyNID)" .
            ""
    } );

    $sql = "insert into names (name, NID) values " .
        substr('(?,?), ' x keys %{$this->{NID}}, 0, -2);
    my @pairs = ();
    while(my ($NID, $name) = each %{$this->{NID}}) {
        push @pairs, $NID, $name;
    }
    $this->{dbh}->do($sql, {}, @pairs );
    
    $sql = 'insert into fields ' .
              '(FID, hasName, typeNID, nameNID, keyNID, fieldType) values';
    my @places = ();
    for my $hasName (keys %{$this->{FID}}) {
    for my $type (keys %{$this->{FID}{$hasName}}) {
    for my $name (keys %{$this->{FID}{$hasName}{$type}}) {
    for my $key  (keys %{$this->{FID}{$hasName}{$type}{$name}}) {
        my ($FID, $fieldType) =
            @{$this->{FID}{$hasName}{$type}{$name}{$key}};
        $sql .= '(?, ?, ?, ?, ?, ?), ';
        my @NIDs = 
            map { $this->{NID}{$_} } ($type, $name, $key);
        push @places, $FID, $hasName, @NIDs, $fieldType;
    }
    }
    }
    }
    $sql = substr($sql,0,-2);
    $this->{dbh}->do($sql, {}, @places );        
    $this->{dbh}->commit;
    
    _recreate($dbh, 'values_double', {
        columns => 
            "webId $fobIdType not null, " .
            "fobId $fobIdType not null, " .
            "ducktype tinyint not null, " .
            "FID $FIDtype not null, " .
            "value double not null, " . # hence table name
            "constraint fobId_by_valuesDouble " .
                "primary key (fobid, ducktype, FID), " .
            "index valuesDouble_by_fobid (ducktype, FID, value)"
    } );
    _recreate($dbh, 'values_datetime', {
        columns => 
            "webId $fobIdType not null, " .
            "fobId $fobIdType not null, " .
            "ducktype tinyint not null, " .
            "FID $FIDtype not null, " .
            "value datetime not null, " . # hence table name
            "constraint fobId_by_valuesDatetime " .
                "primary key (fobid, ducktype, FID), " .
            "index valuesDatetime_by_fobid (ducktype, FID, value)"
    } );
    _recreate($dbh, 'values_text', {
        columns => 
            "webId $fobIdType not null, " .
            "fobId $fobIdType not null, " .
            "ducktype tinyint not null, " .
            "FID $FIDtype not null, " .
            "value $valuesType not null, " . # hence table name
            "constraint fobId_by_valuesString " .
                "primary key (webid, fobid, ducktype, FID), " .
            "index valuesString_by_fobid (ducktype, FID, value($nameLen)), " .
            "fulltext index values_fulltext (value)",
        engine => "MyISAM"
    } );

    _recreate($dbh, 'metaText', {
        columns => 
            "webId $fobIdType not null, " .
            "fobId $fobIdType not null, " .
            "ducktype tinyint not null, " .
            "lnum mediumint not null, " .
            "value $valuesType not null, " .
            "constraint fobId_by_metaText " .
                "primary key (webid, fobid, ducktype, lnum) "
    } );

    _recreate($dbh, 'attachFOB', {
        columns => 
            "fobId $fobIdType not null, " .
            "current tinyint not null, " .
            "FID $FIDtype not null, " .
            "attachId $fobIdType not null, " .
            "seq int unsigned not null, " .
            "constraint seq_by_attachFOB " .
                "primary key (fobid, current, FID, attachId), " .
            "index attachFOB_by_FID (FID)"
    } );

    _recreate($dbh, 'access', {
        columns => 
            "webId $fobIdType not null, " .
            "fobId $fobIdType not null, " .
            "permission tinyint not null, " . # 1=DENY, 2=ALLOW, 3=DENY (This deny is used when ALLOW given to indicate DENY the rest)
            "context char not null, " .     # R=Root, W=Web, T=Topic
            "mode char(12) not null, " .    # VIEW, CHANGE, RENAME and whatever else comes along
            "topicNID $NIDtype not null, " .
            "accessNID $NIDtype not null, " .
            "constraint accessPK " .
                "primary key (webId, fobId, permission, context, mode, accessNID), " .
            "index fobId_by_access (mode, context, accessNID, permission)"
    } );

    _recreate($dbh, 'xref', {
        columns => 
            "fobId $fobIdType not null, " .
            "FID $FIDtype not null, " .
            "xref_fobId $fobIdType not null, " .
            "xref_FID $FIDtype not null, " .
            "count int unsigned not null, " .
            "constraint xref_by_fob " .
                "primary key (fobid, FID, xref_fobId, xref_FID), " .
            "unique index fob_by_xref (xref_fobId, xref_FID)"
    } );
    
    _recreate($dbh, 'xgraph', {
        columns =>
            "latch   SMALLINT  UNSIGNED NULL, " .
            "origid  BIGINT    UNSIGNED NULL, " .
            "destid  BIGINT    UNSIGNED NULL, " .
            "weight  DOUBLE    NULL, " .
            "seq     BIGINT    UNSIGNED NULL, " .
            "linkid  BIGINT    UNSIGNED NULL, " .
            "KEY (latch, origid, destid) USING HASH, " .
            "KEY (latch, destid, origid) USING HASH",
        engine => "OQGRAPH"
    } );


    _recreate($dbh, 'locks', {
        columns => 
            "webNID $NIDtype not null, " .
            "topicNID $NIDtype not null, " .
            "cUIDNID $NIDtype not null, " .
            "time_dt datetime not null, " .
            "constraint lock_by_topic " .
                "primary key (webNID, topicNID)"
    } );

    _recreate($dbh, 'lease', {
        columns => 
            "webNID $NIDtype not null, " .
            "topicNID $NIDtype not null, " .
            "userNID $NIDtype not null, " .
            "taken_dt datetime not null, " .
            "expires_dt datetime not null, " .
            "constraint lease_by_topic " .
                "primary key (webNID, topicNID), " .
            "index lease_by_expires (expires_dt)"
    } );

    _recreate($dbh, 'changes', {
        columns =>
            "webId $fobIdType not null, " .
            "topicNID $NIDtype not null, " .
            "when_dt datetime not null, " .
            "cuidNID $NIDtype not null, " .
            "version $versionType not null, " .
            "constraint changes_by_when " .
                "primary key (webId, when_dt)"
    } );

    return;
}

# SQL in general ignores trailing spaces for key matching and
# lookup, also chars > key length
# So we behave the same way when caching names

# An alternative strategy with spaces is to convert and store
# with an alternate unicode space and convert to from the store
# However, that requires losing that space from our unicode
# repetiore, maybe null would work x00?
# I hope that's not really an issue - but I could be wrong

sub _forEachField {
#    my ( $meta, $fnName, $fnMeta,  $fnCroak ) = @_;
    my ( $meta, $fnName, $fnMeta ) = @_;

    foreach my $type ( keys %$meta ) {
        if(length($type) > $nameLen) {
            next;
        }
        my $etype = "$type$EoV";
        my $data = $meta->{$type};
        if(ref($data) eq 'ARRAY') {
            my $nameSeq = -1;
            foreach my $datum (@$data) {
                $nameSeq++;
                my $name = $datum->{name};
                my $ename = defined $name ? "$name$EoV" : undef;
                &$fnName( $etype, $ename,
                    sprintf($nameSeqFormat, $nameSeq)
                );
                foreach my $key ( grep { $_ ne 'name' } keys %$datum ) {
                    my $ekey = "$key$EoV";
                    &$fnMeta( $etype, $ename, $ekey, $datum->{$key} );
                }
            }
        }
    }
}

sub _commit {
    my $this = shift;
    $this->{dbh}->commit if !$this->{dbh}->{AutoCommit};
}

sub _cacheName {
    my ($store, $NID, $name) = @_;
    $store->{NID}{$name} = $NID;
    $store->{name}{$NID} = $name;
}

sub _addNIDs {
    my $store = shift;
    for my $NID (@_) {
        $store->{missing}{$NID} = 1;
    }
}

sub _readNIDs {
    my ($store) = @_;
    return unless %{$store->{missing}}; # Nothing missing nothing to do
    
    my $sql = "select name, NID from names where NID in (" .
        substr('?,' x keys %{$store->{missing}}, 0, -1) . ')';
    my $rows = $store->{dbh}->selectall_arrayref($sql, {}, ( keys %{$store->{missing}} ) );
    for my $row (@$rows) {
        my ($name, $NID) = @$row;
        $store->{NID}{$name} = $NID;
        $store->{name}{$NID} = $name;
        delete $store->{missing}{$name};
    }
}

# The names passed must already have $EoV at the end
sub _addNames {
    my $store = shift;
    for my $name (@_) {
        if(defined $name) {
            my $n = $name;
            if(!defined $store->{NID}{$n}) {
                if($n =~ m/$nameSeqRegex/ ) {
                    $store->{missing}{$n} = substr($n,0,-1) +1;
                }
                else {
                    $store->{missing}{$n} = 0;
                }
            }
        }
    }
}

sub _readNames {
    my ($store) = @_;
    return unless %{$store->{missing}}; # Nothing missing nothing to do
    
    my $sql = "select name, NID from names where name in (" .
        substr('?,' x keys %{$store->{missing}}, 0, -1) . ')';
    my $rows = $store->{dbh}->selectall_arrayref($sql, {}, ( keys %{$store->{missing}} ) );
    for my $row (@$rows) {
        my ($name, $NID) = @$row;
        $store->{NID}{$name} = $NID;
        $store->{name}{$NID} = $name;
        delete $store->{missing}{$name};
    }
}
    
sub _insertNames {
    my ($store) = @_;
    return unless %{$store->{missing}}; # Nothing missing nothing to do

    # Q: Ignore duplicates because of concurrent inserts?
    #    This is part of a process that will only insert entries that
    #    are new, however with multiple processes running another process
    #    could insert the same entry we are about to, thus casuing this
    #    insert to potentially fail.
    #
    #    Not that we do not care if it is already inserted, from the point of
    #    view of gaining a unique id, if the insert fails because it's
    #    a duplicate, then we will discover that id when we query against
    #    the values we attempted to insert, i.e. we only care that somebody
    #    has inserted that new value and given it an id.
    #    A failed insert is more problematic. The problem is that we're maybe 
    #    inserting many new fields, which one stopped the insert (and caused
    #    a rollback). How to re-try on failure (with an entry removed)?
    #
    # A1: Will the DB just cope? - probably not
    #
    # A2: Use 'on duplicate key update' clause below and do a no-op update
    #     Pros: Fast and simple
    #     Cons: MySQL syntax
    #           I'm not clear that it will work properly for multiple
    #           inserts. Certainly there are caveats on the web. OTOH
    #           this is I think a case that will work
    #
    # A3: Use table locks
    #     Pros: More standard SQL
    #           No duplicates possible
    #     Cons: Slower (although I suspect that's marginal)
    #           Possibility of deadlocks? But not if we code right (not hard)
    #
    # For now I'm on A2 (to make progress), but I need to prove A2 is OK
    # or move to A3
    
    my $sql = "insert into names (name, NID) values " .
        substr('(?,?), ' x keys %{$store->{missing}}, 0, -2);
    my @names = (%{$store->{missing}});
    $store->{dbh}->do($sql . " on duplicate key update name = name", {}, @names );
}
  
sub _addFID {
    my ($store, $hasName, $type, $name, $key) = @_;

    $store->{missing}{$hasName}{$type}{$name}{$key} = 1
        if !defined $store->{FID}{$hasName}{$type}{$name}{$key};
}

sub _readFIDinfo {
    my ($store) = @_;
    my $sql = 'select FID, hasName, typeNID, ' .
              'nameNID, keyNID, fieldtype '. 
              'from fields where';
    my @places = ();
    my @keylist;
    for my $hasName (keys %{$store->{missing}}) {
    for my $type (keys %{$store->{missing}{$hasName}}) {
    for my $name (keys %{$store->{missing}{$hasName}{$type}}) {
        @keylist = keys %{$store->{missing}{$hasName}{$type}{$name}};
        if(@keylist) {
            my @keyNIDs = map { $store->{NID}{$_} } @keylist;

            $sql .= ' hasName = ? and typeNID = ? and nameNID = ? ' .
                'and keyNID in (' . 
                substr('?,' x scalar @keyNIDs, 0, -1) .
                ') OR';
            push @places, $hasName, $store->{NID}{$type}, 
                 $store->{NID}{$name}, @keyNIDs;
        }
    }
    }
    }
    return if !@places;
    $sql = substr($sql,0,-3); # remove last and superfluous 'OR'
    my $sth = $store->{dbh}->prepare($sql);
    $sth->execute(@places);
    my $rows = $sth->fetchall_arrayref;
    for my $row (@$rows) {
        my ($FID, $hasName, $typeNID, $nameNID, $keyNID, $fieldType) = @$row;
        my ($type, $name, $key) = 
            map { $store->{name}{$_} } ($typeNID, $nameNID, $keyNID);

        $store->{FID}{$hasName}{$type}{$name}{$key} = [$FID, $fieldType ];
        $store->{FIDinfo}{$FID} = [$hasName, $type, $name, $key, $fieldType];
        delete $store->{missing}{$hasName}{$type}{$name}{$key};
    }
}

sub _insertFIDinfo {
    my ($store, $fType) = @_;

    my $sql = 'insert into fields ' .
              '(hasName, typeNID, nameNID, keyNID) values';
    my @places = ();
    for my $hasName (keys %{$store->{missing}}) {
    for my $type (keys %{$store->{missing}{$hasName}}) {
    for my $name (keys %{$store->{missing}{$hasName}{$type}}) {
    for my $key  (keys %{$store->{missing}{$hasName}{$type}{$name}}) {
        $sql .= '(?, ?, ?, ?), ' ;
        my @NIDs = 
            map { $store->{NID}{$_} } ($type, $name, $key);
        push @places, $hasName, @NIDs;
    }
    }
    }
    }
    if(@places) {
        $sql = substr($sql,0,-2); # remove last and superfluous ', '
        my $sth = $store->{dbh}->do( $sql . " on duplicate key update keyNID = KeyNID", {}, @places );
    }
}

# $version: 0 for current topic, >= 1 specific version
sub _topicInfo {
    my ($this, $webName, $topicName, $version) = @_;
    return undef unless defined $webName && $webName ne '';
    return undef unless defined $topicName && $topicName ne '';

    $webName =~ s#\.#/#go;
    $version = 0 if !$version;
    
   return $this->{topicInfo}{$webName}{$topicName}{$version}
        if exists $this->{topicInfo}{$webName}{$topicName}{$version};

    my ($sql, @places);
    $sql =
        "select " .
            "fobid, namespace, webid, topic.NID as NID, version, " .
            "date as datestr, unix_timestamp(date) as date, " .
            "author as authorNID, author.name as author, " .
            "comment as commentNID, comment.name as comment, " .
            "reprev " . 
        "from FOBinfo topic, names comment, names author " .
        "where webid = " .
        "(select fobid from FOBinfo " . 
            "where namespace = $namespace{latestWebs} " .
            "and webid = $rootId " .
            "and NID = (select NID from names where name = ?)" .
        ") " .
        "and topic.NID = (select NID from names where name = ?) " .
        "and author = author.NID and comment = comment.NID " .
        "and namespace " .
        (!$version # ie latest 
            ? "= $namespace{latestTopics}"
            : "in ($namespace{latestTopics}, $namespace{otherTopics}) " .
              "and version >= ?"
        );
    @places = ("$webName$EoV", "$topicName$EoV");
    push @places, $version if $version;

#    print "====================================\n";
#    print "$sql\n";
#    print "====================================\n";
#    print "@places\n";
#    print "====================================\n";
#    
    my $sth = $this->{dbh}->prepare_cached($sql);
    $sth->execute(@places);
    my $fobids = $sth->fetchall_arrayref({});

    if(@{$fobids}) {
        my $fi = $fobids->[0];
        $this->_cacheName($fi->{NID}, "$topicName$EoV");
        $this->_cacheName($fi->{authorNID}, $fi->{author});
        $this->_cacheName($fi->{commentNID}, $fi->{comment});
        chop($fi->{author}, $fi->{comment});

        $fi->{webName} = $webName;
        $fi->{topicName} = $topicName;

        my $user = $fi->{author}
            || $Foswiki::Users::BaseUserMapping::UNKNOWN_USER_CUID;
        $fi->{user} = $user;

        $this->{topicInfo}{$webName}{$topicName}{$fi->{version}} = $fi;
        $this->{topicInfo}{$webName}{$topicName}{0} = $fi
            if $fi->{namespace} == $namespace{latestTopics};
        $this->{FOBinfo}{$fi->{fobid}} = $fi;

        return $fi;
    }
    $this->{topicInfo}{$webName}{$topicName}{$version} = undef; # Cache that it's not found
    return undef;
}

sub _webId {
    my ($this, $webName) = @_;
    return $this->{webId}{$webName} if exists $this->{webId}{$webName};

    my $sql = "select fobid, NID from FOBinfo " . 
        "where namespace = $namespace{latestWebs} " .
        "and webid = $rootId " .
        "and NID = (select NID from names where name = ?)";
    my $sth = $this->{dbh}->prepare_cached($sql);
    $sth->execute("$webName$EoV");
    my $rows = $sth->fetchall_arrayref( {} );

    if(@{$rows}) {
        my $webId = $rows->[0]{fobid};
        $this->{webName}{$webId} = $webName;
        $this->{webId}{$webName} = $webId;

        $this->{NID}{"$webName$EoV"} = $rows->[0]{NID}; 
        $this->{name}{$rows->[0]{NID}} = "$webName$EoV"; 
        return $webId;
    }
        
    $this->{webId}{$webName} = 0; # Cache that it's not found
    return 0;
}

sub _fixUp_PREFs {
    my ($this, $meta) = @_;

    my $prefs = Foswiki::Prefs::TopicRAM->new($meta);

    # Must clear, passed $meta may have been unloaded and old values remain
    $meta->{_PREF_SET} = [];
    $meta->{_PREF_LOCAL} = [];    

    my $pref_set = -1;
    my @preflist = $prefs->prefs();
    for my $p (@preflist) {
        my $value = $prefs->get($p);
        $meta->{_indices}{_PREF_SET}{$p} = ++$pref_set;
        $meta->{_PREF_SET}[$pref_set] =
            { name => $p, value => $value };            
#            { name => $p, type => 'Set', value => $value };            
    }

    my $pref_local = -1;
    my @locallist = $prefs->localPrefs();
    for my $p (@locallist) {
        my $value = $prefs->getLocal($p);
        $meta->{_indices}{_PREF_LOCAL}{$p} = ++$pref_local;
        $meta->{_PREF_LOCAL}[$pref_local] =
            { name => $p, value => $value };            
#            { name => $p, type => 'Local', value => $value };            
    }
    return;
}

sub _fixUp_ACLs {
    my ($this, $meta) = @_;
    
    my %acl;
    for my $p (@{$meta->{_PREF_SET}}) {
        my $rule = $p->{name};

        # SMELL: Is this test definitive, does the core provide such a regex?
        next if $rule !~ m{^(ALLOW|DENY)(ROOT|WEB|TOPIC)([A-Z]+)$};
        my ($permission, $context, $mode) = ($1 , $2, $3);

        my %list = map { $_ => 1 } 
            grep { /^(?!%)\S/ } # Only non-ws, plus first char not %
            map {
                s/^(Main|%USERSWEB%|%MAINWEB%)\.//;
            $_  } split( /[,\s]+/, $p->{value} );

        $acl{$mode}{$permission}{$context} = [] if scalar keys(%list) == 0; # Needed for DENYTOPIC
        for my $access_id (keys %list) {
            push @{$acl{$mode}{$permission}{$context}}, $access_id;
        }            
    }
    # Check for any ALLOW_context_mode = SomeList and re-interpret as DENY_context_mode = EveryBodyNotInSomeList
    #     YES that's *weird* see VersatileACLAccess.pm for more
    # Check for DENYTOPIC_mode = '' and re-interpret as ALLOWTOPIC_mode = AnyBodyGroup
    #     Note this is deliberately only done for context = TOPIC
    # 
    # Further pruning is alas not possible, for example
    #   DENYTOPIC  = A, B, C, F
    #   ALLOWTOPIC = A, B, C, DGroup
    #
    # Consideration was given to collapse this to:
    #   DENYTOPIC  =
    #   ALLOWTOPIC = DGroup
    # The logic being A, B, C & F are not D therefore denied, however 
    # If DGroup = A, F, X, Y, Z then originally X, Y, Z had access but after pruning A, F, X, Y, Z have access
    #
    # There is the special case
    #   DENYTOPIC  = A, B, C
    #   ALLOWTOPIC = A, B, C
    # (that is identical lists) are equivalent to deny all, which could be optimised (but I suspect it's rare and I haven't done that!)
    
    for my $mode (keys %acl) {
        my ($dt, $at) = ( $acl{$mode}{DENY}{TOPIC}, $acl{$mode}{ALLOW}{TOPIC} );
        if($dt && (scalar @{$dt} == 0)) { # DENY has priority over ALLOW
            $acl{$mode}{ALLOW}{TOPIC} = [ '' ]; # '' cannot be a valid cUID so used as a logical AnyBodyGroup
            undef $acl{$mode}{DENY}{TOPIC};
        }
        elsif($at && (scalar @{$at} >= 1)) {
            push @{$acl{$mode}{'DENY?'}{TOPIC}}, ''; # '' cannot be a valid cUID so used as a logical EveryBodyNotInSomeList
        }
        my $aw = $acl{$mode}{ALLOW}{WEB};
        if($aw && (scalar @{$aw} >= 1)) {
            push @{$acl{$mode}{'DENY?'}{WEB}}, ''; # '' cannot be a valid cUID so used as a logical EveryBodyNotInSomeList
        }
        my $ar = $acl{$mode}{ALLOW}{ROOT};
        if($ar && (scalar @{$ar} >= 1)) {
            push @{$acl{$mode}{'DENY?'}{ROOT}}, ''; # '' cannot be a valid cUID so used as a logical EveryBodyNotInSomeList
        }
    }

    my @accessors;
    for my $mode (keys %acl) {
        for my $permission (keys %{$acl{$mode}}) {
        for my $context (keys %{$acl{$mode}{$permission}}) {
            for my $access_Id (@{$acl{$mode}{$permission}{$context}}) {
                next if !defined $acl{$mode}{$permission}{$context};
                my $p = $permission eq 'DENY' ? 1 : $permission eq 'ALLOW' ? 2 : 3;
                push @accessors, [ "$access_Id$EoV", $p, substr($context,0,1), $mode ];
                $this->_addNames("$access_Id$EoV");
            }
        }
        }
    }

    return \@accessors;
}

#############################################################################
# REST HANDLERS
#############################################################################

# Basically check the DBI connect strings have been correctly defined in configure
# TODO: Also required as a configure checker
sub _restCONNECT {
    my $store = Foswiki::Store::Versatile->new();
    return "$store->{errstr}\n\n";
}

# TODO: also as a config checker or at least reported?
sub _restDBI {
    use DBI;
    print "Installed DBI drivers and data sources\n";
    my %drivers = DBI->installed_drivers;
    for my $driver (keys %drivers) {
        my @data_sources = DBI->data_sources($driver);
        for my $ds (@data_sources) {
            print "DB: $driver ++ $drivers{$driver}, DSN: $ds\n";
        }
    }
    return "";
}

# Note that MariaDB 10.0.5+ extends regex support to include PCRE
# The MySQL reqex notes here http://www.regular-expressions.info/mysql.html
# reference a PCRE library that provides similar support
#
# The MariaDB support includes utf8 and the MySQL library can be
# compiled to have Unicode support: see http://www.regular-expressions.info/pcre.html
#
# Therefore, I am only looking to tweak the regex passed thru to the
# underlying DB. According to the link above MySQL uses POSIX ERE
# which according to http://foswiki.org/Development/RegularExpressions
# means this is substantially close to requirements
#
# Therefore, I am not busting a gut to fully 'defuse' the MySQL/MariaDB
# and Foswiki differences. The soon arriving PCRE support will fix
# most of the issues and those it doesn't will be easier to difuse.
#
# One extra piece of work required is that it's only possible to
# return either just the topic name or the topic name + whole metaText.
# That is, it's not possible to return the lines within metaText that
# grep would match. The extra PCRE support may allow that further
# optimsation.
#
# In addition, there will be Versatile 'query' eventually which will
# create SQL queries directly. Thus leaving this code for actual regex
# search of topic text rather than meta data, although there will still
# be a legacy issue of old %SEARCHes.

sub _search {
    my ( $this, $searchRAW, $web, $inputTopicSet, $session, $options, $srch ) = @_;

    # $options->{type}    Either eq regex or ne regex
    # $options->{wordboundaries}

    # ====== $options->{casesensitive} ======
    # MySQL regex does not support a way to be insensitive (or vice versa)
    # either way, we will need to post-apply another filter here.
    # $options->{casesensitive} This also means return the metaText for further perl processing
    
    # ====== $options->{files_without_match} ======
    # If I read the grep man page right, then we are actually using the grep -l option which
    # is also known as --files-with-matches as opposed to -L which means --files-without-match.
    # That is to say (FW files_without_match == grep files-with-matches) which is confusing but correct.
    
    # For grep both (--files-with-matches and --files-without-matches) only return a list of
    # distinct files that match (with) or do not match (without) as opposed to grep providing
    # the filenames *AND* the matching line of text (repeated for each match).

    # In a FW context the point of view is different. Normally a %SEARCH will return the
    #    Matching *files* (i.e topic names) *WITH* the lines of the topic text which *matches*
    # But when files_without_match is given to search
    #    Matching *files* (i.e topic names) *WITHOUT* the 1st line of topic text which *matches*
    #    That is to say just a list of matching topic names

    # There must be benefits either way depending on the _search callers needs setting this
    # option if appropriate
    
    # if ( $options->{files_without_match} ) # Just give me matching topic list
    # else { # Give me matching lines of text as well
    #          SQL must return the whole text and re-use regex in perl to capture lines we want

    my $searchString = $searchRAW; 

    if( defined $srch ) { }
    elsif ( $options->{type} && $options->{type} eq 'regex' ) {
          if( $searchString eq '^---[+][^+][^\r\n]+[\r\n]' ) { $srch = $searchString; }
       elsif( $searchString eq 'META:FORM.*?name="TW4AssociateForm' ) { $srch = 'META:FORM.*name="TW4AssociateForm'; }
       elsif( $searchString =~ /[\^]?\Q%META:\E[A-Z0-9_]*\Q{.*\bname=\"\E/ ) { ($srch = $searchString) =~ s/\\b//go; }
       elsif( $searchString =~ /[\^]?\Q%META:\E[A-Z0-9_]*\Q[{].*\bname=\"\E/ ) { ($srch = $searchString) =~ s/\\b//go; }
       elsif( $searchString =~ /[\^]?\Q%META:\E[A-Z0-9_]*\Q{.*name=\"\E/ ) { $srch = $searchString; }
       elsif( $searchString =~ /[\^]?\Q%META:\E[A-Z0-9_]*\Q[{].*name=\"\E/ ) { $srch = $searchString; }
       elsif( $searchString =~ /[\^]?%META:(.*?)\[?\{\]?/ ) { $srch = "^%META:$1\{"; } 
       else { $srch = '.'; }
    }
    else {
        $srch = $searchString;
    }

    if ( $options->{type} && $options->{type} eq 'regex' ) {

        # Escape /, used as delimiter. This also blocks any attempt to use
        # the search string to execute programs on the server.
        $searchString =~ s!/!\/!g;
    }
    else {
        # Escape non-word chars in search string for plain text search
        $searchString =~ s/(\W)/\\$1/g;
    }

    $searchString =~ s/^(.*)$/\[\[:space:\]\]\*$1\[\[:space:\]\]\*/go if $options->{'wordboundaries'};
    
    print STDERR "PerlSerch($searchString)\nSQLsearch($srch)\n\n" if $srch eq '.';

    my $maxTopicsInSet = 10000;    # max number of topics for an SQL select
    
    my @set;
    my %seen;
    my $webid = $this->_webId($web);
    $inputTopicSet->reset();
    while ( $inputTopicSet->hasNext() ) {
        my $webtopic = $inputTopicSet->next();
        my ( $Iweb, $tn ) =
          Foswiki::Func::normalizeWebTopicName( $web, $webtopic );
        push( @set, "$tn$EoV" );
        if (
            ( $#set >= $maxTopicsInSet )
            || !( $inputTopicSet->hasNext() )
          )
        {
            my $sql =
                "select topic.name as topic, m.value as mtext from metaText m, FOBinfo fob, names topic " . 
                    "where m.fobid = fob.fobid and fob.NID = topic.NID " . # Join metaText with FOBinfo and Names (for topic names)
                    "and m.value regexp ? " .
                    "and m.ducktype = 0 " .
                    "and m.webid = ? " . 
                    "and m.fobid in ( " . 
                        "select fobid from FOBinfo where namespace = $namespace{latestTopics} and webid = ? " .
                            "and NID in " . 
                            "(select NID from names where name in (" . substr('?,' x (scalar @set),0,-1) . "))" .
                    ") order by topic, m.lnum";
            my $sth = $this->{dbh}->prepare_cached($sql);
            $sth->execute( $srch, $webid, $webid, @set );
            my $matches = $sth->fetchall_arrayref( {} );

            my $count = 0;
            for my $match (@{$matches}) {
                my $topic = $match->{topic};
                chop $topic;
                my $lin = $match->{mtext};
                if( $options->{casesensitive} ) {
                    next if $lin !~ /$searchString/;
                }
                else {
                    next if $lin !~ /$searchString/i;
                }
                $count++;
                push @{ $seen{$topic} }, $lin;
                next if $options->{files_without_match};
            }
        }
    }
    return \%seen;
}

} # End 'persistence' block
1;
__END__
Foswiki - The Free and Open Source Wiki, http://foswiki.org/

Copyright (C) 2012 Julian Levens

This program is free software; you can redistribute it and/or
modify it under the terms of the GNU General Public License
as published by the Free Software Foundation; either version 2
of the License, or (at your option) any later version. For
more details read LICENSE in the root of this distribution.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.

As per the GPL, removal of this notice is prohibited.