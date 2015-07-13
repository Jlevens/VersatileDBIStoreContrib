sub _recreateTables {
$text .= _recreate($dbh,"fobID","fobid bigint unsigned auto_increment primary key");
    # $text .= _recreate($dbh,"web","id mediumint unsigned auto_increment unique key, name varchar(190), constraint primary key (name)");
    
    # The length of name can be increased to 188, by leaving at 180 I have 30 bytes available for the future
    # fob type is one of:
    #
    #      1: Root (and webid = 0)
    #      2: Web  (and webid = 0)
    #      3: Shadow Web (and webid = 0)
    #      4: Topic (and webid = fobid of a previously created Web)
    #      5: Shadow Topic (and webid = fobid of a previously created Web)
    # Also add 16 if it's only a FOB reference (from another topic - the real topic is not real yet)

#    $text .= _recreate($dbh,"FOB","fobid bigint unsigned, webid bigint unsigned, name varchar(180), type tinyint unsigned, lease varchar(200), constraint primary key (webid, name)");
    $text .= _recreate($dbh,"FOB","fobid bigint unsigned, webid bigint unsigned, name varchar(180), type tinyint unsigned, create_id bigint unsigned, info_date timestamp, info_version int unsigned, info_comment varchar(300), info_author varchar(200), lease varchar(200), constraint primary key (webid, name)");
    $text .= _reindex($dbh,"","FOB_byType","FOB","type"); # NB not a unique index, type will match many Webs/Topics
#    $text .= _reindex($dbh,"","FOB_by_info_date","FOB","info_date"); # NB not a unique index, info_datem will match many Webs/Topics
    
    # How did I solve the sequence problem?
    #   * That is to say that all META:TYPE repeated (made unique by name) in a topic should be returned as an array in Foswiki::Meta maintaining the order
    #   * Comes for free in text files, need to work at it for SQL
    #
    # Basic solution is that each META:TYPE{name= is assigned a values_string in hex (remember utf-8, hence cannot use binary actually could use 7-bit chars to be compact, but I need to be aware of utf8 collation order)
    #   * top 4 bytes are used for the main sequence: hence 4 billion entries (must start at one not zero, to allow room above)
    #   * extra characters can be added after this at any time to allow for the (future) possibility to insert new entries without resequencing as this will cause much versioning work
    #   * I've used values_string as that's where most data is kept as it provides good locality
    #   * It also serves to indicate the presence of the relavent META:TYPE{name even in the absence of any other keys and values, (and will still be ordered properly)
    #   * Remember META:TYPE{name is a unique field_id, therefore the value had no clear meaning, until now
    
    # The max key length in MySQL is 767 bytes which is only 191 utf8mb4 characters (767 / 4 = 191 chars remainder 3 bytes)
    #
    # META:VERSATILEDBISTORELONGMETANAME
    # META:123456789-123456789-123456789-
    #
    # Note that the primary key is not the field-id, this is to allow repeats to define aliases (but do we want them?)
    #
    # field.type is 1 for duck type (i.e. support the initial release) any other value is to extend fields either for new dataform field types or mapping to existing SQL tables
    # field.type is 2 for xref.reason
    # field.type is 3 are for fields mapped to FOB table
    # field.type is 4 are for fields mapped to other tables
    #
    # metamember contains the value of the name="..." part of META:TYPE{name="Abc" key1="x" key2="y" etc} OR
    #                                                         META:TYPE{name="" key1="x" key2="y"} OR
    #                                                         META:TYPE{key1="x" key2="y"}
    # In the last two cases Foswiki::Meta treats name as "" i.e they are the same unique key. Other metamember is set to "Abc" to form the unique key
    # 
    $text .= _recreate($dbh,"fields","id int unsigned not null auto_increment unique key, type tinyint not null default 1, metatype varchar(35), hasMetamember tinyint unsigned, metamember varchar(140), metakey varchar(25) not null, constraint field_pk primary key (metatype, hasMetamember, metamember, metakey)");
        

       
    # type (ALLOW/DENY), RWT (ROOT/WEB/TOPIC) and mode (VIEW/CHANGE) in the following are specially created 'field-id's
    # access_id is a fobid which is also specifically one-of:
    #    * cUID
    #    * GroupTopic (a unique id for the group)
    #    * no-one (a logical Group)
    #    * any-one (a logical Group)
    #    * XxxWebDenyMODEGroup \ logically created one per Web and MODE combination
    #    * XxxWebAllowMODEGroup } which simply maps the list of cUIDs and groups in the WebPreference setting
    #    * XxxWebPermitMODEGroup / Despite being called permit, it's value can be 'D' for denied
#    $text .= _recreate($dbh,"access","fobid bigint unsigned, access_id bigint unsigned, consent int, object int, mode int, constraint primary key (consent, object, mode, access_id, fobid)");
    $text .= _recreate($dbh,"access","fobid bigint unsigned, access_id bigint unsigned, rule varchar(50), constraint primary key (rule, access_id, fobid)");
    $text .= _reindex($dbh,"","access_TopicNUser","access","fobid, access_id"); # NB not a unique index, same Topic & User/Group with diff access rules



    
    # DuckType: 1=Number (& string), 2=Date (& string),  3=Date or number (& string), 4=String only
    # Will I need to consider an item like "5 pounds" as a number (from sorting/indexing point of view?). If so, is that a another duck-type: prefixed-number?
    # With ducktype before value then sorting will naturally be in separate (albeit sorted) blocks. Will the DB be smart enough to merge sort these blocks?
    # This can be fixed by adding another index which excludes ducktype, this will also speed up pure string searches
    #
    # A search term of item op 23.5, would first search values_double and select matches, then search values_string with ducktype in (2,4) i.e dates and strings (non-nums)
    # A search term of item op date(), would first search values_datetime and select matches, then search values_string with ducktype in (1,4) i.e nums and strings (non-dates)
    #
    # Foswiki::Time::parseTime accepts any integer >= 60 as a year and therefore all of these will be seen as both date and number. However,
    # this is probably rather aggressive for most sites. Therefore a config option to say only treat integers in range xxx..yyyy as years and hence dates
    # for indexing purposes a range of 0..0 to never treat integers as a possible date, hence no field will be seen as a date and number at the same time.
    
    # Are 21000 (utf8) characters enough for any META field in FW? The largest I can think of are text-areas for dataforms but that's pretty big. I could
    # change this to a MEDIUMTEXT (about 5MB but that's stored separately hence slower - probably offset by 251 bytes being stored and retrieved directly in the index)
    
    $text .= _recreate($dbh,"values_string","fobid bigint unsigned, field_id int unsigned, ducktype tinyint unsigned, value mediumtext, constraint primary key (fobid, field_id, value(180))","MyISAM");
    $text .= _reindex($dbh,"","values_string_fieldNvalue","values_string","field_id, value(180)"); # NB not a unique index, same field & value possible in diff topics
    # $text .= _reindex($dbh,"fulltext","values_string_fts","values_string","value");
    #my $str = "Julian Mark Levens" x 1166;
    #my $sth = $dbh->prepare("insert into values_string values(100,1,4,?);");
    #$sth->execute($str);
    
    # Used for Duck-indexing of data that looks like a number
    $text .= _recreate($dbh,"values_double","fobid bigint unsigned, field_id int unsigned, value double, constraint primary key (fobid, field_id, value)");
    $text .= _reindex($dbh,"","values_double_fieldNvalue","values_double","field_id, value"); # NB not a unique index, same field & value possible in diff topics
    
    # Used for Duck-indexing of data that looks like a date [time]
    $text .= _recreate($dbh,"values_datetime","fobid bigint unsigned, field_id int unsigned, value datetime, constraint primary key (fobid, field_id, value)");
    $text .= _reindex($dbh,"","values_datetime_fieldNvalue","values_datetime","field_id, value"); # NB not a unique index, same field & value possible in diff topics
    #
    # For future use (actually storing dates), please note:
    # 'YYYY-MM-DD HH:MM:SS' format for MySQL (and other flavours?) not really what we want. To support ISO8601 fully I'll need extra columns for MySQL, other SQL flavours
    # have timezone support
    # I'll also need to ensure I can index this properly (datetime at same point in time are index/sorted as such even if recorded with diff TZ)
    # MySQL does have a CONVERT_TZ function, not sure if this helps
    #
    # Conversely, FW only has loose date[time] types at the moment. Therefore for transitioning this may be OK.    
    
    # $text .= _recreate($dbh,"values_text","fobid bigint, field_id int, value text, constraint primary key (fobid, field_id, value(180))");
        
    # Tables yet to create
    #     name_table: with normal & reverse index
    #
    # Need to think clearly about primary indexes
    #                             secondary indexes - possibly unique
    #                             foreign keys to establish

    # field_id and xref_field can be 0 to indicate a reference just to a particular FOB rather than a particular field within a FOB
    # I wonder about the possibility of a xref_id (a la field table) to distinguish distinct types of xref. That would require another xref_id table and so on.
    # In practice I think that this is not necessary as the combination of field_id and xref_field will probably serve the same purpose. Indeed, if the combination
    # is duplicated what would be the benefit of providing a further distinction (no I am not clear on this idea).
    $text .= _recreate($dbh,"xref","fobid bigint unsigned, field_id int unsigned, xref_fobid bigint unsigned, xref_field_id int unsigned, reason int unsigned, constraint primary key (fobid, field_id, xref_fobid, xref_field_id, reason)");
    $text .= _reindex($dbh,"unique","xref_reverse","xref","xref_fobid, xref_field_id, fobid, field_id, reason");

# Possible Version tables ====================================================================
        
# Benefits of this table are some isolation of different SQL flavours and all entries are clearly in sequence.
# Two 'simultaneous' updates to the same topic could occur, but one would hit this first and get the lower id.
# The time is probably redunant, except for extra info
# 
# Relying on timestamps accurate to microseconds is not strictly a guarantee you can disambuiguate. This method will disambiguate
# many changes on the same day even with no time element at all.
#
#    $text .= _recreate($dbh,"timestamp","id unsigned int auto_increment primary key, date datetime, time int");
        
    $text .= "\n\n";
    
    # Pre-define a few field-ids
    my $fid;
    $fid = $this->_insertField('_text','','');
    
    # Ideally scan through Foswiki::Meta::VALIDATE and pre-define the fields that we can


    # The standard access 'Fields'
    
#    $this->_insertField('_access','consent','ALLOW');
#    $this->_insertField('_access','consent','DENY');
#
#
#    $this->_insertField('_access','object','ROOT');
#    $this->_insertField('_access','object','WEB');
#    $this->_insertField('_access','object','TOPIC');
#
#    
#    $this->_insertField('_access','mode','VIEW');
#    $this->_insertField('_access','mode','CHANGE');
#    $this->_insertField('_access','mode','RENAME');
    
    
    $this->_insertTopic(0,"<NobodyGroup>");
    $this->_insertTopic(0,"<EverybodyGroup>");  # I think this works better overall than <AnyBody>
}