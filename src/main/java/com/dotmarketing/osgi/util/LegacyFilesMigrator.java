package com.dotmarketing.osgi.util;

import java.io.IOException;
import java.util.List;

import com.dotmarketing.beans.Host;
import com.dotmarketing.beans.Identifier;
import com.dotmarketing.beans.Permission;
import com.dotmarketing.beans.VersionInfo;
import com.dotmarketing.business.APILocator;
import com.dotmarketing.business.CacheLocator;
import com.dotmarketing.business.IdentifierAPI;
import com.dotmarketing.business.PermissionAPI;
import com.dotmarketing.business.UserAPI;
import com.dotmarketing.business.VersionableAPI;
import com.dotmarketing.db.HibernateUtil;
import com.dotmarketing.exception.DotHibernateException;
import com.dotmarketing.portlets.contentlet.business.ContentletAPI;
import com.dotmarketing.portlets.contentlet.business.HostAPI;
import com.dotmarketing.portlets.contentlet.model.Contentlet;
import com.dotmarketing.portlets.files.business.FileAPI;
import com.dotmarketing.portlets.files.model.File;
import com.dotmarketing.portlets.languagesmanager.business.LanguageAPI;
import com.dotmarketing.portlets.structure.business.StructureAPI;
import com.dotmarketing.portlets.structure.model.Structure;
import com.dotmarketing.util.Logger;
import com.liferay.portal.model.User;
import com.liferay.util.FileUtil;

public class LegacyFilesMigrator{
    
    private static ContentletAPI contAPI;
    private static LanguageAPI langAPI;
    private static HostAPI hostAPI;
    private static FileAPI fileAPI;
    private static IdentifierAPI identifierAPI;
    private static PermissionAPI permissionAPI;
    private static StructureAPI stAPI;
    private static UserAPI userAPI;
    private static VersionableAPI versionableAPI;
    
    private static User sysUser;
    private static Structure fileAssetContentType;
    
    public LegacyFilesMigrator(){
        contAPI = APILocator.getContentletAPI();
        langAPI = APILocator.getLanguageAPI();
        hostAPI = APILocator.getHostAPI();
        fileAPI = APILocator.getFileAPI();
        identifierAPI = APILocator.getIdentifierAPI();
        permissionAPI = APILocator.getPermissionAPI();
        stAPI = APILocator.getStructureAPI();
        userAPI = APILocator.getUserAPI();
        versionableAPI = APILocator.getVersionableAPI();
    }
    
    
    public void migrateLegacyFiles(){
        
        try {
            
            sysUser = userAPI.getSystemUser();
            fileAssetContentType = stAPI.findByVarName("FileAsset", sysUser);

            int migrated = 0;
            List<Host> hostsList = hostAPI.findAll(sysUser, false);
            for (Host h : hostsList){
                Logger.info(this.getClass(), "Migrating files for Site:" + h.getHostname());
                List<File> filesPerSite = fileAPI.getAllHostFiles(h, false, sysUser, false);
                for (File f: filesPerSite){
                    if(migrated == 0)
                        HibernateUtil.startTransaction();
                    //Migrate every file per site
                    Logger.info(this.getClass(), "Migrating file: " + f.getURI());
                    if(migrateLegacyFile (f)){
                        migrated++;
                    }
                    if(migrated == filesPerSite.size() || (migrated > 0 && migrated % 100 == 0)){
                        HibernateUtil.commitTransaction();
                    }
                }
                Logger.info(this.getClass(), "All Legacy files under site " + h.getHostname() +" have been migrated.");
            }
            Logger.info(this.getClass(), "All Legacy files have been migrated. Please undeploy this plugin.");
        } catch(Exception ex) {
            try {
                Logger.error(this.getClass(), "There was a problem migrating Files to Contents: " + ex.getMessage(), ex );
                HibernateUtil.rollbackTransaction();
            } catch (DotHibernateException e1) {
                Logger.warn(this, e1.getMessage(),e1);
            }
        }
        finally {
            try {
                HibernateUtil.closeSession();
            } catch (DotHibernateException e) {
                Logger.error(this.getClass(), "Something happened: ",e);
            }
        }
    }
    
    public boolean migrateLegacyFile (File file) throws Exception{

        Identifier legacyident=identifierAPI.find(file);
        VersionInfo vInfo=versionableAPI.getVersionInfo(legacyident.getId());
        
        File working=(File) versionableAPI.findWorkingVersion(legacyident, sysUser, false);
        java.io.File fileReferenceInFS = fileAPI.getAssetIOFile(file);
        
        Contentlet cworking = migrateLegacyFileData(file), clive=null;
        setHostFolderValues(cworking, legacyident);
        
        if(vInfo.getLiveInode()!=null && !vInfo.getLiveInode().equals(vInfo.getWorkingInode())) {
            File live=(File) versionableAPI.findLiveVersion(legacyident, sysUser, false);
            clive = migrateLegacyFileData(live);
            setHostFolderValues(clive, legacyident);   
        }
        
        List<Permission> perms=null;
        if(!permissionAPI.isInheritingPermissions(working)) {
            perms = permissionAPI.getPermissions(working, true, true, true);
        }

        fileAPI.delete(working, sysUser, false);
        fileReferenceInFS.delete();

        HibernateUtil.getSession().clear();
        CacheLocator.getIdentifierCache().removeFromCacheByIdentifier(legacyident.getId());

        if(clive!=null) {
            Contentlet cclive = contAPI.checkin(clive, sysUser, false);
            contAPI.publish(cclive, sysUser, false);
        } else {
            Contentlet ccworking = contAPI.checkin(cworking, sysUser, false);
            if(vInfo.getLiveInode()!=null && vInfo.getLiveInode().equals(ccworking.getInode())) {
                contAPI.publish(ccworking, sysUser, false);
            }
        }

        return true;
    }
    
    private java.io.File copyFileToTempFolder (File file) {
        java.io.File tmp=new java.io.File(fileAPI.getRealAssetPathTmpBinary()
                + java.io.File.separator + file.getModUser()
                + java.io.File.separator + System.currentTimeMillis()
                + java.io.File.separator + file.getFileName());
        if(tmp.exists())
            tmp.delete();
        try {
            java.io.File originalFile = fileAPI.getAssetIOFile(file);
            if(originalFile.exists())
                FileUtil.copyFile(originalFile, tmp);
        } catch (IOException e) {
            Logger.error( this.getClass(), "Error processing Stream", e );
        }
        return tmp;
    }
    
    private Contentlet migrateLegacyFileData(File file ) throws Exception{
        Contentlet newCon = new Contentlet();
        newCon.setStructureInode(fileAssetContentType.getInode());
        newCon.setLanguageId(langAPI.getDefaultLanguage().getId());
        newCon.setInode(file.getInode());
        newCon.setIdentifier(file.getIdentifier());
        newCon.setModUser(file.getModUser());
        newCon.setModDate(file.getModDate());

        newCon.setStringProperty("title", file.getFileName());
        newCon.setStringProperty("fileName", file.getFileName());
        newCon.setStringProperty("description", file.getTitle());
        
        java.io.File tmp = copyFileToTempFolder(file);
        newCon.setBinary("fileAsset", tmp);
        
        return newCon;
    }
    
    private void setHostFolderValues(Contentlet con, Identifier legacyident) throws Exception{
        con.setHost(legacyident.getHostId());
        con.setFolder(APILocator.getFolderAPI().findFolderByPath(
                legacyident.getParentPath(), legacyident.getHostId(), sysUser, false).getInode());
    }
}