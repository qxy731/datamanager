package com.myboot.dataprocess.process.ignite;

import java.io.Serializable;

/**
 * @author mengjie
 */
public class VersionCache implements Serializable {
    private static final long serialVersionUID = 1L;

    private Object data;
    private int ver;

    public static VersionCache of(Object data, int ver) {
        return new VersionCache(data, ver);
    }

    public Object getData() {
        return data;
    }

    public void setData(Object data) {
        this.data = data;
    }

    public int getVer() {
        return ver;
    }

    public void setVer(int ver) {
        this.ver = ver;
    }

    VersionCache(Object data, int ver) {
        super();
        this.data = data;
        this.ver = ver;
    }

    VersionCache() {
        super();
    }

    private enum EMPTY {
        VC(new VersionCache());
        VersionCache vc;

        private EMPTY(VersionCache vc) {
            this.vc = vc;
        }

        VersionCache get() {
            return vc;
        }
    }

    public static VersionCache empty() {
        return EMPTY.VC.get();
    }

}
