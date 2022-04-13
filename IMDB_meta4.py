

def IMDB_meta4(soup_second, data_output, i):
    try:
        meta4 = soup_second.find('div', {'id': "fullcredits_content"})
        k = 0
        for factor in meta4.findAll('h4'):
            k = k + 1
        name = meta4.findAll('h4')
        table = meta4.findAll('table')
    except: pass
    try:
        for counter in range(0, k):
            name1 = name[counter].text
            table1 = table[counter].text
            ######### Directed #########
            try:
                if 'Directed' in name1:
                    director = ''
                    try:
                        for j in table[counter].findAll('a'):
                            director = j.text + ',' + director
                        data_output.loc[i, 'Director'] = director
                    except:
                        director = table1
                        data_output.loc[i, 'Director'] = director
            except: pass
            ######### Produced ######### 
            try:
                if 'Produced' in name1:
                    producer = ''
                    try:
                        for j in table[counter].findAll('a'):
                            producer = j.text + ',' + producer
                        data_output.loc[i, 'Producer'] = producer
                    except:
                        producer = table1
                        data_output.loc[i, 'Producer'] = producer
            except: pass
            ######### Cast #########
            try:
                if 'Cast' in name1 and 'Casting' not in name1:
                    cast = ''
                    for j in table[counter].findAll('tr'):
                        jj = j.findAll('td')
                        try:
                            cast = jj[1].text + ',' + cast
                        except: pass
                    data_output.loc[i, 'Casts'] = cast
                    if cast == '':
                        cast = table1
                        data_output.loc[i, 'Casts'] = cast
            except: pass
            ######### Writing #########
            try:
                if 'Writing' in name1:
                    writer = ''
                    try:
                        for j in table[counter].findAll('a'):
                            writer = j.text + ',' + writer
                        data_output.loc[i, 'Writer'] = writer
                    except:
                        writer = table1
                        data_output.loc[i, 'Writer'] = writer
            except: pass
            ######### Cinematography #########
            try:
                if 'Cinematography' in name1:
                    cinematography = ''
                    try:
                        for j in table[counter].findAll('a'):
                            cinematography = j.text + ',' + cinematography
                        data_output.loc[i, 'Cinematography'] = cinematography
                    except:
                        cinematography = table1
                        data_output.loc[i, 'Cinematography'] = cinematography
            except: pass
            ######### Editing #########
            try:
                if 'Editing' in name1:
                    editing = ''
                    try:
                        for j in table[counter].findAll('a'):
                            editing = j.text + ',' + editing
                        data_output.loc[i, 'Editor'] = editing
                    except:
                        editing = table1
                        data_output.loc[i, 'Editor'] = editing
            except: pass
            ######### Costume Design #########
            try:
                if 'Costume Design' in name1:
                    costumedesign = ''
                    try:
                        for j in table[counter].findAll('a'):
                            costumedesign = j.text + ',' + costumedesign
                        data_output.loc[i, 'CostumeDesign'] = costumedesign
                    except:
                        costumedesign = table1
                        data_output.loc[i, 'CostumeDesign'] = costumedesign
            except: pass
            ######### Makeup Department #########
            try:
                if 'Makeup Department' in name1:
                    makeupdepartment = ''
                    try:
                        for j in table[counter].findAll('a'):
                            makeupdepartment = j.text + ',' + makeupdepartment
                        data_output.loc[i, 'MakeupDepartment'] = makeupdepartment
                    except:
                        makeupdepartment = table1
                        data_output.loc[i, 'MakeupDepartment'] = makeupdepartment
            except: pass
            ######### Production Management #########
            try:
                if 'Production Management' in name1:
                    productionmanagement = ''
                    try:
                        for j in table[counter].findAll('a'):
                            productionmanagement = j.text + ',' + productionmanagement
                        data_output.loc[i, 'ProductionManagement'] = productionmanagement
                    except:
                        productionmanagement = table1
                        data_output.loc[i, 'ProductionManagement'] = productionmanagement
            except: pass
            ######### Music by #########
            try:
                if 'Music by' in name1:
                    musicby = ''
                    try:
                        for j in table[counter].findAll('a'):
                            musicby = j.text + ',' + musicby
                        data_output.loc[i, 'MusicBy'] = musicby
                    except:
                        musicby = table1
                        data_output.loc[i, 'MusicBy'] = musicby
            except: pass
            ######### Visual Effects by #########
            try:
                if 'Visual Effects by' in name1:
                    visualeffectsby = ''
                    try:
                        for j in table[counter].findAll('a'):
                            visualeffectsby = j.text + ',' + visualeffectsby
                        data_output.loc[i, 'VisualEffectsBy'] = visualeffectsby
                    except:
                        visualeffectsby = table1
                        data_output.loc[i, 'VisualEffectsBy'] = visualeffectsby
            except: pass
            ######### Second Unit Director or Assistant Director #########
            try:
                if 'Second Unit Director' in name1:
                    seconddirector = ''
                    try:
                        for j in table[counter].findAll('a'):
                            seconddirector = j.text + ',' + seconddirector
                        data_output.loc[i, 'SecondDirector'] = seconddirector
                    except:
                        seconddirector = table1
                        data_output.loc[i, 'SecondDirector'] = seconddirector
            except: pass
            ######### Camera and Electrical Department #########
            try:
                if 'Camera and Electrical Department' in name1:
                    cameraelectrical = ''
                    try:
                        for j in table[counter].findAll('a'):
                            cameraelectrical = j.text + ',' + cameraelectrical
                        data_output.loc[i, 'CameraElectrical'] = cameraelectrical
                    except:
                        cameraelectrical = table1
                        data_output.loc[i, 'CameraElectrical'] = cameraelectrical
            except: pass
            ######### Editorial Department #########
            try:
                if 'Editorial Department' in name1:
                    editorialdepartment = ''
                    try:
                        for j in table[counter].findAll('a'):
                            editorialdepartment = j.text + ',' + editorialdepartment
                        data_output.loc[i, 'EditorialDepartment'] = editorialdepartment
                    except:
                        editorialdepartment = table1
                        data_output.loc[i, 'EditorialDepartment'] = editorialdepartment
            except: pass
            ######### Music Department #########
            try:
                if 'Music Department' in name1:
                    musicdepartment = ''
                    try:
                        for j in table[counter].findAll('a'):
                            musicdepartment = j.text + ',' + musicdepartment
                        data_output.loc[i, 'MusicDepartment'] = musicdepartment
                    except:
                        musicdepartment = table1
                        data_output.loc[i, 'MusicDepartment'] = musicdepartment
            except: pass
            ######### Sound Department #########
            try:
                if 'Sound Department' in name1:
                    sounddepartment = ''
                    try:
                        for j in table[counter].findAll('a'):
                            sounddepartment = j.text + ',' + sounddepartment
                        data_output.loc[i, 'SoundDepartment'] = sounddepartment
                    except:
                        sounddepartment = table1
                        data_output.loc[i, 'SoundDepartment'] = sounddepartment
            except: pass
            ######### Art Department #########
            try:
                if 'Art Department' in name1:
                    artdepartment = ''
                    try:
                        for j in table[counter].findAll('a'):
                            artdepartment = j.text + ',' + artdepartment
                        data_output.loc[i, 'ArtDepartment'] = artdepartment
                    except:
                        artdepartment = table1
                        data_output.loc[i, 'ArtDepartment'] = artdepartment
            except: pass
            ######### Special Effects by #########
            try:
                if 'Special Effects by' in name1:
                    specialeffects = ''
                    try:
                        for j in table[counter].findAll('a'):
                            specialeffects = j.text + ',' + specialeffects
                        data_output.loc[i, 'SpecialEffects'] = specialeffects
                    except:
                        specialeffects = table1
                        data_output.loc[i, 'SpecialEffects'] = specialeffects
            except: pass
            ######### Art Direction by #########
            try:
                if 'Art Direction by' in name1:
                    artdirection = ''
                    try:
                        for j in table[counter].findAll('a'):
                            artdirection = j.text + ',' + artdirection
                        data_output.loc[i, 'ArtDirection'] = artdirection
                    except:
                        artdirection = table1
                        data_output.loc[i, 'ArtDirection'] = artdirection
            except: pass
            ######### Animation Department #########
            try:
                if 'Animation Department' in name1:
                    animationdepartment = ''
                    try:
                        for j in table[counter].findAll('a'):
                            animationdepartment = j.text + ',' + animationdepartment
                        data_output.loc[i, 'AnimationDepartment'] = animationdepartment
                    except:
                        animationdepartment = table1
                        data_output.loc[i, 'AnimationDepartment'] = animationdepartment
            except: pass
            ######### Stunts #########
            try:
                if 'Stunts' in name1:
                    stunts = ''
                    try:
                        for j in table[counter].findAll('a'):
                            stunts = j.text + ',' + stunts
                        data_output.loc[i, 'Stunts'] = stunts
                    except:
                        stunts = table1
                        data_output.loc[i, 'Stunts'] = stunts
            except: pass
            ######### Casting By #########
            try:
                if 'Casting By' in name1:
                    castingby = ''
                    try:
                        for j in table[counter].findAll('a'):
                            castingby = j.text + ',' + castingby
                        data_output.loc[i, 'CastingBy'] = castingby
                    except:
                        castingby = table1
                        data_output.loc[i, 'CastingBy'] = castingby
            except: pass
            ######### Set Decoration by #########
            try:
                if 'Set Decoration by' in name1:
                    setdecoration = ''
                    try:
                        for j in table[counter].findAll('a'):
                            setdecoration = j.text + ',' + setdecoration
                        data_output.loc[i, 'SetDecoration'] = setdecoration
                    except:
                        setdecoration = table1
                        data_output.loc[i, 'SetDecoration'] = setdecoration
            except: pass
            ######### Casting Department #########
            try:
                if 'Casting Department' in name1:
                    castingdepartment = ''
                    try:
                        for j in table[counter].findAll('a'):
                            castingdepartment = j.text + ',' + castingdepartment
                        data_output.loc[i, 'CastingDepartment'] = castingdepartment
                    except:
                        castingdepartment = table1
                        data_output.loc[i, 'CastingDepartment'] = castingdepartment
            except: pass
            ######### Costume and Wardrobe Department #########
            try:
                if 'Costume and Wardrobe Department' in name1:
                    costumewardrobe = ''
                    try:
                        for j in table[counter].findAll('a'):
                            costumewardrobe = j.text + ',' + costumewardrobe
                        data_output.loc[i, 'CostumeWardrobe'] = costumewardrobe
                    except:
                        costumewardrobe = table1
                        data_output.loc[i, 'CostumeWardrobe'] = costumewardrobe
            except: pass
            ######### Location Management #########
            try:
                if 'Location Management' in name1:
                    locationmanagement = ''
                    try:
                        for j in table[counter].findAll('a'):
                            locationmanagement = j.text + ',' + locationmanagement
                        data_output.loc[i, 'LocationManagement'] = locationmanagement
                    except:
                        locationmanagement = table1
                        data_output.loc[i, 'LocationManagement'] = locationmanagement
            except: pass
            ######### Script and Continuity #########
            try:
                if 'Script and Continuity' in name1:
                    scriptcontinuity = ''
                    try:
                        for j in table[counter].findAll('a'):
                            scriptcontinuity = j.text + ',' + scriptcontinuity
                        data_output.loc[i, 'ScriptContinuity'] = scriptcontinuity
                    except:
                        scriptcontinuity = table1
                        data_output.loc[i, 'ScriptContinuity'] = scriptcontinuity
            except: pass
    except: pass
    return data_output  



















