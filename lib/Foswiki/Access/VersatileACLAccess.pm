# See bottom of file for license and copyright information

# When deciding whether to grant access, Foswiki evaluates the following rules in order (read from the top of the list; if the logic arrives at PERMITTED or DENIED that applies immediately and no more rules are applied). You need to read the rules bearing in mind that VIEW, CHANGE and RENAME access may be granted/denied separately. 
#
# Settings are only read from the most current (latest) revision of a topic. Settings from older revisions are never used, even when viewing an older revision with the rdiff script
#
# If the user is an administrator access is PERMITTED. 
# If DENYTOPIC is set to a list of wikinames 
#   + people in the list will be DENIED. 
# If DENYTOPIC is set to empty ( i.e. Set DENYTOPIC = ) 
#   + access is PERMITTED i.e no-one is denied access to this topic.
#       * Attention: Use this with caution. This is deprecated and will
#       * likely change in the next release.
# If ALLOWTOPIC is set 
#   + people in the list are PERMITTED 
#   + everyone else is DENIED 
# If DENYWEB is set to a list of wikinames 
#   + people in the list are DENIED access 
# If ALLOWWEB is set to a list of wikinames 
#   + people in the list will be PERMITTED 
#   + everyone else will be DENIED 
# If you got this far, access is PERMITTED

=pod

---+ package Foswiki::Access::TopicACLAccess

Implements the traditional, longstanding ACL in topic preference style.

=cut

package Foswiki::Access::TopicACLAccess;

use Foswiki::Access;
@ISA = qw(Foswiki::Access);

use constant MONITOR => 0;

use strict;
use Assert;

use Foswiki          ();
use Foswiki::Address ();
use Foswiki::Meta    ();
use Foswiki::Users   ();

my $EoV = '\0'; # Smell need to keep in sync with Foswiki::Store::Versatile 

sub new {
    my ( $class, $session ) = @_;
    ASSERT( $session->isa('Foswiki') ) if DEBUG;
    my $this = bless( { session => $session }, $class );

    return $this;
}

=begin TML

---++ ObjectMethod haveAccess($mode, $User, $web, $topic, $attachment) -> $boolean
---++ ObjectMethod haveAccess($mode, $User, $meta) -> $boolean
---++ ObjectMethod haveAccess($mode, $User, $address) -> $boolean

   * =$mode=  - 'VIEW', 'CHANGE', 'CREATE', etc. (defaults to VIEW)
   * =$cUID=    - Canonical user id (defaults to current user)
Check if the user has the given mode of access to the topic. This call
may result in the topic being read.

=cut

sub haveAccess {
    my ( $this, $mode, $cUID, $param1, $param2, $param3 ) = @_;
    $mode ||= 'VIEW';
    $cUID ||= $this->{session}->{user};

    my $session = $this->{session};
    undef $this->{failure};
    # super admin is always allowed

    if ( $session->{users}->isAdmin($cUID) ) {
        print STDERR "$cUID - ADMIN\n" if MONITOR;
        return 1;
    }

    my $meta;

    if ( ref($param1) eq '' ) {

        #scalar - treat as web, topic
        $meta = Foswiki::Meta->load( $session, $param1, $param2 );
        ASSERT( not defined($param3) )
          if DEBUG
          ;    #attachment ACL not currently supported in traditional topic ACL
    }
    else {
        if ( ref($param1) eq 'Foswiki::Address' ) {
            $meta =
              Foswiki::Meta->load( $session, $param1->web(), $param1->topic() );
        }
        else {
            $meta = $param1;
        }
    }
    ASSERT( $meta->isa('Foswiki::Meta') ) if DEBUG;

    print STDERR "Check $mode access $cUID to " . $meta->getPath() . "\n"
      if MONITOR;

    my ($web, $topic) = ($meta->web, $meta->topic);
    if(!$web && !$topic) { # Root access

        # No web, we are checking at the root. Check DENYROOT and ALLOWROOT.
        $deny = $this->_getACL( $meta, 'DENYROOT' . $mode );

        if ( defined($deny)
            && $session->{users}->isInUserList( $cUID, $deny ) )
        {
            $this->{failure} =
              $session->i18n->maketext('access denied on root');
            print STDERR 'e ' . $this->{failure}, "\n" if MONITOR;
            return 0;
        }

        $allow = $this->_getACL( $meta, 'ALLOWROOT' . $mode );

        if ( defined($allow) && scalar(@$allow) != 0 ) {
            unless ( $session->{users}->isInUserList( $cUID, $allow ) ) {
                $this->{failure} =
                  $session->i18n->maketext('access not allowed on root');
                print STDERR 'f ' . $this->{failure}, "\n" if MONITOR;
                return 0;
            }
        }
    }
 
    my $webPermission = $this->{WP}{$meta->{_web}}{$cUID};
    
    if(!defined $webPermission) {
        $webPermisison = 1;
        $deny = $this->_getACL( $meta, 'DENYWEB' . $mode );
        if ( defined($deny)
            && $session->{users}->isInUserList( $cUID, $deny ) )
        {
            $webPermission = 0;
        }
        
        $allow = $this->_getACL( $meta, 'ALLOWWEB' . $mode );
        
        if ( defined($allow) && scalar(@$allow) != 0 ) {
            unless ( $session->{users}->isInUserList( $cUID, $allow ) ) {
                $webPermission = 0;
            }
        }
        $this->{WP}{$web}{$cUID} = $webPermission;
    }
    return $webPermission if !$topic;

    $mode = uc($mode);
    
    my @members;
    if(!exists $this->{membership}{$cUID}) {
        # SMELL: is $cUID returned by eachMembership anyway?
        # '' represents the AnyBody group and everyone is part of that group
        @members = map { "$_$EoV"; } ($session->{users}->eachMembership($cUID)->all, $cUID, '');
        $this->{membership}{$cUID} = \@members;
    }
    
    if(!$this->{topicsLoaded}{$web}{$cUID}) {
        $this->{topicsLoaded}{$web}{$cUID} = 1;
        my $webId = $store->_webId($web);
        my $groups =  substr('?,' x scalar @{$this->{membership}{$cUID}},0,-1);
        my $sql = "select topic.name as topic, permission from access, names topic where topicNID = topic.NID " . 
            "and webid = ? and mode = ? " . 
            "and accessId in (select NID from names where name in ($groups)) " .
            "group by fobid, permission desc" .
            "";
        my $sth = $store->{dbh}->prepare_cached($sql);
        $sth->execute($webId, $mode, @members);
        my $acls = $sth->fetchall_arrayref({});
        
        for my $acl @{$acls} {
            my $aclTopic = substr($acl->{topic},0,-1);
            if(!defined $this->{TP}{$web}{$aclTopic}{$cUID} ) {
                $this->{TP}{$web}{$aclTopic}{$cUID} = ($acl->{permission} eq 'A' ? 1 : 0);
            }
        }
    }

    return defined $this->{TP}{$web}{$topic}{$cUID} ? $this->{TP}{$web}{$topic}{$cUID} : $webPermission;
}

# Get an ACL preference. Returns a reference to a list of cUIDs, or undef.
# If the preference is defined but is empty, then a reference to an
# empty list is returned.
# This function canonicalises the parsing of a users list. Is this the right
# place for it?
sub _getACL {
    my ( $this, $meta, $mode ) = @_;

    if ( defined $meta->topic && !defined $meta->getLoadedRev ) {

        # Lazy load the latest version.
        $meta->loadVersion();
    }

    my $text = $meta->getPreference($mode);
    return undef unless defined $text;

    # Remove HTML tags (compatibility, inherited from Users.pm
    $text =~ s/(<[^>]*>)//g;

    # Dump the users web specifier if userweb
    my @list = grep { /\S/ } map {
        s/^($Foswiki::cfg{UsersWebName}|%USERSWEB%|%MAINWEB%)\.//;
        $_
    } split( /[,\s]+/, $text );

    #print STDERR "getACL($mode): ".join(', ', @list)."\n";

    return \@list;
}

1;
__END__
Foswiki - The Free and Open Source Wiki, http://foswiki.org/

Copyright (C) 2008-2011 Foswiki Contributors. Foswiki Contributors
are listed in the AUTHORS file in the root of this distribution.
NOTE: Please extend that file, not this notice.

This program is free software; you can redistribute it and/or
modify it under the terms of the GNU General Public License
as published by the Free Software Foundation; either version 2
of the License, or (at your option) any later version. For
more details read LICENSE in the root of this distribution.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.

As per the GPL, removal of this notice is prohibited.
