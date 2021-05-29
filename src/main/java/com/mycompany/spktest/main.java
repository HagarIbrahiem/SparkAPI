/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.mycompany.spktest;

import java.io.IOException;


public class main {
    public static void main(String[] args) throws IOException
    {
        System.out.println("************** Highest Tag Words ******************** " );
        YoutubeTagWordCount.getTagWordCount();
        //System.out.println("************** Highest title Words ******************" );
        //YoutubeTitleWordCount.getTitleWordCount();
    }
}
