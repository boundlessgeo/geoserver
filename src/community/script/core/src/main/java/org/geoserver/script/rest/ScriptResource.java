/* Copyright (c) 2001 - 2012 TOPP - www.openplans.org. All rights reserved.
 * This code is licensed under the GPL 2.0 license, available at the root
 * application directory.
 */
package org.geoserver.script.rest;

import java.io.File;
import java.io.IOException;

import org.geoserver.rest.RestletException;
import org.geoserver.script.ScriptManager;
import org.restlet.data.MediaType;
import org.restlet.data.Request;
import org.restlet.data.Response;
import org.restlet.data.Status;
import org.restlet.resource.FileRepresentation;
import org.restlet.resource.Resource;

import static java.lang.String.format;

/**
 * Resource for the contents of a script.
 * 
 * @author Justin Deoliveira, OpenGeo
 */
public class ScriptResource extends Resource {

    ScriptManager scriptMgr;
    String path;

    public ScriptResource(ScriptManager scriptMgr, String path, Request request, Response response) {
        super(null, request, response);
        this.scriptMgr = scriptMgr;
        this.path = path;
    }

    @Override
    public void handleGet() {
        File script;
        try {
            script = scriptMgr.findScriptFile(path);
        } catch (IOException e) {
            throw new RestletException(format("Error looking up script %s", path),
                Status.SERVER_ERROR_INTERNAL, e);
        }
        if (script == null) {
            throw new RestletException(format("Could not find script %s", path), 
                Status.CLIENT_ERROR_NOT_FOUND);
        }

        //TODO: set different content type?
        //TODO: not sure about this time to live parameter...  
        getResponse().setEntity(new FileRepresentation(script, MediaType.TEXT_PLAIN, 10));
    }
}
