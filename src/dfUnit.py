import sys
from .utils import *


class dfUnit:
    def __init__(self,hash):
        self.hash   = hash  # typically hash_top + "#" + hash_bottom
        self.flist  = []    # list of catalog files with the same hash
        self.fsize  = -1    # calculated size of each file
        self.ndup   = -1    # files in flist with same size, but different inode (they already have the same hash)
        self.ndup_files = -1 # number of duplicate files ... used for counting duplicate files
        self.ndup_inode = -1 # number of duplicate inodes ... used for counting duplicate bytes
        self.size_set   = set()    # set of unique sizes ... if len(size_set) then split is required
        self.calculated_size_list = []
        self.uid_list   = []        # list of uids of files added
        self.inode_list = []        # list of inodes of files added
        self.oldest_inode   = -1    # oldest_ ... is for the file which is NOT the duplicate or is the original
        self.oldest_index   = -1
        self.oldest_age     = -1
        self.oldest_uid     = -1

    def nfiles_with_hash(self): # return number of files in this hash (total ... all users included)
        return len(self.flist)

    def add_fd(self,fd):
        # add the file to flist
        self.flist.append(fd)
        # add size if not already present
        self.size_set.add(fd.size)
        self.calculated_size_list.append(fd.calculated_size)
        # add uid
        self.uid_list.append(fd.uid)
        # add inode
        self.inode_list.append(fd.inode)
        # update oldest file
        if fd.mtime > self.oldest_age: # current file is older than known oldest
            self.oldest_age     = fd.mtime
            self.oldest_index   = len(self.flist) - 1 # oldest_index is the index of the item last added
            self.oldest_uid     = fd.uid
            self.oldest_inode   = fd.inode

    def filter_flist_by_uid(self,uid):
        for i,f in enumerate(self.flist):
            if f.uid == uid : self.keep.append(i)
                
    def compute(self,hashhashsplits): 
        # find if files have the same hashes, but different sizes then...
        #   1. split them into different hashes by size and 
        #   2. append them to hashhashsplits
        # else ... aka .. .no spliting is required
        #   1. count number of duplicate inodes and
        #   2. size of each file
        split_required = False
        # check if spliting is required
        if len(self.size_set) > 1: # more than 1 size in this hash
            split_required = True
            for i,size in enumerate(self.size_set):
                tophash, bottomhash = self.hash.split("#")
                bottomhash += "_" + str(i)
                newhash = "#".join([tophash,bottomhash])        # this is the newhash for this size
                hashhashsplits[newhash] = dfUnit(newhash)
                for fd in self.flist:
                    if fd.size == size:
                        hashhashsplits[newhash].add_fd(fd)
        else: # there only 1 size ... no splits required
            self.ndup   = len(self.inode_list) - 1  # ndup is zero if same len(size_set)==1 and len(inode_list)==1
            self.ndup_inode = len(set(self.inode_list)) - 1
            self.ndup_files = len(self.inode_list) - 1 # some duplicate files may be hard links
            self.fsize  = self.flist[0].calculated_size
        return split_required
    
    def get_user_file_index(self,uid):
        uid_file_index = []
        if not uid in self.uid_list:
            if uid == 0: uid_file_index = list(range(0,len(self.flist))) # uid == 0 is all users
            return uid_file_index
        else:
            for i,j in enumerate(self.flist):
                if j.uid == uid: uid_file_index.append(i)
            return uid_file_index


    
    def __str__(self):
        return "{0} : {1} {2} {3}".format(self.hash, self.ndup_inode, self.fsize,"##".join(map(lambda x:str(x),self.flist)))
        
    def str_with_name(self,uid2uname, gid2gname,findex):
        original_inode = self.flist[findex[0]].inode # first file is the original
        uid_inodes = [self.flist[i].inode for i in findex[1:]] # 2nd file onwards could be duplicates (same file but different inode) or copy (same inode <-> hardlink)
        dup_uid_inodes = list(filter(lambda x:x!=original_inode,uid_inodes))
        ndup_uid_inodes = len(dup_uid_inodes)
        return "{0} : {1} {2} {3} {4}".format(self.hash, 
                                              self.ndup_inode,  # total number of duplicates (all users)
                                              ndup_uid_inodes,  # total number of duplicates (this uid)
                                              self.fsize,       # size of each duplicate
                                              "##".join(map(lambda x:x.str_with_name(uid2uname,gid2gname),[self.flist[i] for i in findex])))

def _get_inode(s):
    s = s.strip().split(";")
    return(s[-8])

def _get_filename_from_fgzlistitem(string):
    string = string.strip().split(";")[:-1]
    for i in range(11):
        dummy = string.pop(-1)
    filename = ";".join(string)
    return filename

class fgz: # used by grubber
    def __init__(self):
        self.hash = ""
        self.ndup_inode = -1        # number of duplicate inodes (all users)
        self.ndup_uid_inode = -1    # number of duplicate inodes (this uid)
        self.filesize = -1          # size of each duplicate or copy
        self.totalsize = -1         # total size of duplicates for this uid
        self.fds = []               # list of duplicate files for this uid
        self.of = ""                # original file aka non-duplicate file
    
    def __lt__(self,other):
        return self.totalsize > other.totalsize
    
    def __str__(self):
        dup_fds = []
        inodes_seen = dict()
        inodes_seen[_get_inode(self.of)] = 1
        for f in self.fds:
            f_inode = _get_inode(f)
            if f_inode in inodes_seen: continue
            inodes_seen[f_inode] = 1
            dup_fds.append(f)
        outstring=[]
        outstring.append(str(self.hash))
        outstring.append(str(self.ndup_uid_inode))
        outstring.append(str(self.totalsize))
        outstring.append(str(self.filesize))
        outstring.append(_get_filename_from_fgzlistitem(self.of))
        outstring.append("##".join(map(lambda x:_get_filename_from_fgzlistitem(x),dup_fds)))
        return "\t".join(outstring)
        # return "{0} {1} {2} {3} {4}".format(self.hash,self.ndup,get_human_readable_size(self.totalsize), get_human_readable_size(self.filesize), ";".join(map(lambda x:_get_filename_from_fgzlistitem(x),self.fds)))
        # return "{0} {1} {2} {3} {4}".format(self.hash,self.ndup,self.totalsize, self.filesize, ";".join(map(lambda x:_get_filename_from_fgzlistitem(x),self.fds)))

# 09f9599cff76f6c82a96b042d67f81ff#09f9599cff76f6c82a96b042d67f81ff : 158 1348 "/data/CCBR/projects/ccbr583/Pipeliner/.git/hooks/pre-push.sample";1348;41;8081532070425347857;1;1552;35069;57786;jailwalapa;CCBR;##"/data/CCBR/projects/ccbr785/FREEC/.git/hooks/pre-push.sample";1348;41;11610558684702129747;1;1629;35069;57786;jailwalapa;CCBR;##"/data/CCBR/projects/ccbr785/citup/pypeliner/.git/hooks/pre-push.sample";1348;41;9306919632329364056;1;1624;35069;57786;jailwalapa;CCBR;##"/data/CCBR/projects/ccbr785/titan_workflow/.git/hooks/pre-push.sample";1348;41;7658100918611057517;1;1628;35069;57786;jailwalapa;CCBR;##"/data/CCBR/rawdata/ccbr1016/batch1/fastq/scratch/example/Pipeliner/.git/hooks/pre-push.sample";1348;41;328973360624494807;1;1253;35069;57786;jailwalapa;CCBR;##"/data/CCBR/rawdata/ccbr1040/Seq2n3n4n5_GEXnHTO/Pipeliner/.git/hooks/pre-push.sample";1348;41;16190385205193530167;1;1093;35069;57786;jailwalapa;CCBR;##"/data/CCBR/rawdata/ccbr1044/Pipeliner/.git/hooks/pre-push.sample";1348;41;10429578581567757002;1;1110;35069;57786;jailwalapa;CCBR;    
    def set(self,inputline):
        original_line = inputline
        try:
            inputline = inputline.strip().split(" ")
            if len(inputline) < 6:
                raise Exception("Less than 6 items in mimeo.files.gz line.")
            self.hash = inputline.pop(0)
            dummy = inputline.pop(0) # the colon
            self.ndup_inode = int(inputline.pop(0))
            self.ndup_uid_inode = int(inputline.pop(0))
            if self.ndup_uid_inode == 0: # may be mimeo was run to output all files .. not just dups .. aka without the -z option
                return False
            self.filesize = int(inputline.pop(0))
            full_fds = " ".join(inputline) # bcos file names can contain spaces
            fds = full_fds.split("##")
            self.of = fds.pop(0)    # one file is the original
            self.fds = fds          # other duplicates or copies
            self.totalsize = self.ndup_uid_inode * self.filesize    # total size of all duplicates for this uid
            return True
        except:
            sys.stderr.write("spacesavers2:{0}:files.gz Do not understand line:{1} with {2} elements.\n".format(self.__class__.__name__,original_line,len(inputline)))
            # exit()            
            return False
