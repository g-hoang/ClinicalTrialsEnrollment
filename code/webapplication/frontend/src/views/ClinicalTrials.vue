<template>
    <div class="wrapper">
        <parallax class="section page-header header-filter" :style="headerStyle">
            <div class="container">
                <div class="md-layout">
                <div
                    class="md-layout-item md-size-50 md-small-size-70 md-xsmall-size-100"
                >
                    <h1 class="title">Forecasting Patient Enrolment for Clinical Trials</h1>
                    <h4>
                    Supervisor: Niklas Frühauf, Sovanta
                    </h4>
                    <br />
                </div>
                </div>
            </div>
        </parallax>
        <div class="main main-raised">
            <div class="section" id="about">
                <div class="container">
                    <h2 class="title text-center">About Clinical Trials</h2>
                    <p class="text-center">
                        Our world is facing with a countless number of diseases, which raise the need in faster development of medical techniques, vaccine or drugs.
                        However, the clinical trials process normally are expensive, regarding to time and financial terms, especially involving in the patient enrollment process.
                        The patient enrollment can be a long and costly process for the institution conducting a clinical trial.
                        <!-- </p>
                    <p class="text-center"> -->
                        On the timely perspective, a clinical study should be completed as soon as possible, because the finished clinical trials from competitors can make ours to be old-fashioned even when it has not completed. On the other hand, optimizing cost always is a mandatory task for any project to avoid unnecessary annoyance and discouragement. It is possible to estimate the duration and cost of the study based on its research design, but the patient enrollment remains a unknown variable. Thus, several studies tried to modelling and predicting enrollment duration and enrollment successful rates with different approaches. <i>What impacts the duration of patient enrollment?</i> and <i>Can it be approximated with data mining techniques?</i>. This project aims to answer these questions by gaining insights on clinical trials and which features are drivers for their duration. Hence, clinical trial records will be collected and preprocessed, to enable training a predictive machine learning algorithm with them.
                    </p>
                </div>
                
            </div>
            <div class="section text-center" id="team">
                <div class="container-fluid">
                    <h2 class="title">Here is our team</h2>
                    <div class="team">
                        <div class="md-layout">
                            <div class="md-layout-item md-medium-size-33 md-small-size-100">
                                <div class="team-player">
                                    <md-card class="md-card-plain">
                                        <div class="md-layout-item md-size-50 mx-auto">
                                        <img
                                            :src="teamImg3"
                                            alt="Thumbnail Image"
                                            class="img-raised rounded-circle img-fluid"
                                        />
                                        </div>
                                        <h4 class="card-title">
                                        Luka Biedebach
                                        <br />
                                        <small class="card-description text-muted">Master Data Science</small>
                                        </h4>
                                    </md-card>
                                </div>
                            </div>

                            <div class="md-layout-item md-medium-size-33 md-small-size-100">
                                <div class="team-player">
                                    <md-card class="md-card-plain">
                                        <div class="md-layout-item md-size-50 mx-auto">
                                        <img
                                            :src="teamImg5"
                                            alt="Thumbnail Image"
                                            class="img-raised rounded-circle img-fluid"
                                        />
                                        </div>
                                        <h4 class="card-title">
                                        Wei-Yi Chen
                                        <br />
                                            <small class="card-description text-muted">Master Business Informatics</small>
                                        </h4>
                                    </md-card>
                                </div>
                            </div>

                            <div class="md-layout-item md-medium-size-33 md-small-size-100">
                                <div class="team-player">
                                    <md-card class="md-card-plain">
                                        <div class="md-layout-item md-size-50 mx-auto">
                                        <img
                                            :src="teamImg2"
                                            alt="Thumbnail Image"
                                            class="img-raised rounded-circle img-fluid"
                                        />
                                        </div>
                                        <h4 class="card-title">
                                        Giang Hoang
                                        <br />
                                            <small class="card-description text-muted">Master Business Informatics</small>
                                        </h4>
                                    </md-card>
                                </div>
                            </div>

                            <div class="md-layout-item md-medium-size-33 md-small-size-100">
                                <div class="team-player">
                                    <md-card class="md-card-plain">
                                        <div class="md-layout-item md-size-50 mx-auto">
                                        <img
                                            :src="teamImg1"
                                            alt="Thumbnail Image"
                                            class="img-raised rounded-circle img-fluid"
                                        />
                                        </div>
                                        <h4 class="card-title">
                                            Carolin Holtermann
                                        <br />
                                        <small class="card-description text-muted">Master Data Science</small>
                                        </h4>
                                    </md-card>
                                </div>
                            </div>

                            <div class="md-layout-item md-medium-size-33 md-small-size-100">
                                <div class="team-player">
                                    <md-card class="md-card-plain">
                                        <div class="md-layout-item md-size-50 mx-auto">
                                        <img
                                            :src="teamImg4"
                                            alt="Thumbnail Image"
                                            class="img-raised rounded-circle img-fluid"
                                        />
                                        </div>
                                        <h4 class="card-title">
                                        Stefan Sousa
                                        <br />
                                            <small class="card-description text-muted">Master Business Informatics</small>
                                        </h4>
                                    </md-card>
                                </div>
                            </div>
                            
                        </div>
                        
                    </div>
                </div>
            </div>
            <div class="section" id="predict">
                <div class="container">
                    <div class="md-layout">
                        <div class="md-layout-item md-size-100">
                            <md-button class="md-default" style="margin-bottom: 30px" v-on:click="predict()">Predict</md-button>
                        </div>

                        <div v-if="options != {}" class="input_components md-layout-item md-size-100 md-layout">

                            <!-- Condition -->
                            <div class="md-layout-item md-size-100 md-layout">
                                <div v-if="options['Condition'] != null" class="md-layout-item md-size-50">
                                    <div v-for="(input, index) in condition" :key="index" class="md-layout">
                                        <md-autocomplete required v-model="input['value']" :md-options="options['Condition']" :md-fuzzy-search="false" class="md-layout-item" style="margin-right: 25px">
                                            <label>Condition or disease</label>
                                        </md-autocomplete>
                                        <md-button class="md-just-icon md-round md-danger" v-if="index!=0" v-on:click="removeField('condition', index)">
                                            <md-icon>remove</md-icon>
                                        </md-button>
                                    </div>
                                </div>
                                <md-button class="md-just-icon md-round" v-on:click="addField('condition');">
                                    <md-icon>add</md-icon>
                                </md-button>
                            </div>

                            <!-- Arm group label -->
                            <!-- <div class="md-layout-item md-size-100 md-layout">
                                <div v-if="options['ArmGroupLabel'] != null" class="md-layout-item md-size-50">
                                    <div v-for="(input, index) in armGroupLabel" :key="index" class="md-layout">
                                        <md-autocomplete required v-model="input['value']" :md-options="options['ArmGroupLabel']" :md-fuzzy-search="false" class="md-layout-item" style="margin-right: 25px">
                                            <label>Arm group label</label>
                                        </md-autocomplete>
                                        <md-button class="md-just-icon md-round md-danger" v-if="index!=0" v-on:click="removeField('armGroupLabel', index);">
                                            <md-icon>remove</md-icon>
                                        </md-button>
                                    </div>
                                </div>
                                <md-button class="md-just-icon md-round" v-on:click="addField('armGroupLabel');">
                                    <md-icon>add</md-icon>
                                </md-button>
                            </div> -->

                            <!-- Condition Ancestor Term -->
                            <div class="md-layout-item md-size-100 md-layout">
                                <div v-if="options['ConditionAncestorTerm'] != null" class="md-layout-item md-size-50">
                                    <div v-for="(input, index) in conditionAncestorTerm" :key="index" class="md-layout">
                                        <md-autocomplete required v-model="input['value']" :md-options="options['ConditionAncestorTerm']" :md-fuzzy-search="false" class="md-layout-item" style="margin-right: 25px">
                                            <label>Condition Ancestor Term</label>
                                        </md-autocomplete>
                                        <md-button class="md-just-icon md-round md-danger" v-if="index!=0" v-on:click="removeField('conditionAncestorTerm', index);">
                                            <md-icon>remove</md-icon>
                                        </md-button>
                                    </div>
                                </div>
                                <md-button class="md-just-icon md-round" v-on:click="addField('conditionAncestorTerm');">
                                    <md-icon>add</md-icon>
                                </md-button>
                            </div>

                            <!-- Condition Browse Branch Abbrev -->
                            <div class="md-layout-item md-size-100 md-layout">
                                <div v-if="options['ConditionBrowseBranchAbbrev'] != null" class="md-layout-item md-size-50">
                                    <div v-for="(input, index) in conditionBrowseBranchAbbrev" :key="index" class="md-layout">
                                        <md-autocomplete required v-model="input['value']" :md-options="options['ConditionBrowseBranchAbbrev']" :md-fuzzy-search="false" class="md-layout-item" style="margin-right: 25px">
                                            <label>Condition Browse Branch Abbrev</label>
                                        </md-autocomplete>
                                        <md-button class="md-just-icon md-round md-danger" v-if="index!=0" v-on:click="removeField('conditionBrowseBranchAbbrev', index);">
                                            <md-icon>remove</md-icon>
                                        </md-button>
                                    </div>
                                </div>
                                <md-button class="md-just-icon md-round" v-on:click="addField('conditionBrowseBranchAbbrev');">
                                    <md-icon>add</md-icon>
                                </md-button>
                            </div>

                            <!-- Condition Mesh Id -->
                            <div class="md-layout-item md-size-100 md-layout">
                                <div v-if="options['ConditionMeshId'] != null" class="md-layout-item md-size-50">
                                    <div v-for="(input, index) in conditionMeshId" :key="index" class="md-layout">
                                        <md-autocomplete required v-model="input['value']" :md-options="options['ConditionMeshId']" :md-fuzzy-search="false" class="md-layout-item" style="margin-right: 25px">
                                            <label>Condition Mesh Id</label>
                                        </md-autocomplete>
                                        <md-button class="md-just-icon md-round md-danger" v-if="index!=0" v-on:click="removeField('conditionMeshId', index);">
                                            <md-icon>remove</md-icon>
                                        </md-button>
                                    </div>
                                </div>
                                <md-button class="md-just-icon md-round" v-on:click="addField('conditionMeshId');">
                                    <md-icon>add</md-icon>
                                </md-button>
                            </div>

                            <h3 class="md-layout-item md-size-100 title">Eligibility Criteria:</h3>

                            <!-- Age group and gender -->
                            <div class="md-layout-item md-size-100 md-layout">
                                <!-- <md-checkbox v-model="stdAge" class="md-primary" value="Child">Child</md-checkbox>
                                <md-checkbox v-model="stdAge" class="md-primary" value="Adult">Adult</md-checkbox>
                                <md-checkbox v-model="stdAge" class="md-primary" value="Older Adult">Older Adult</md-checkbox> -->
                                <div class="md-layout-item md-size-15 padding-left-0">
                                    <md-field>
                                        <label for="minimumAge">Minimum age</label>
                                        <md-input v-model="minimumAge" type="number" name="minimumAge" required></md-input>
                                    </md-field>
                                </div>
                                <div class="md-layout-item md-size-20">
                                    <md-field>
                                        <label for="unitMinAge">Unit for minimum age</label>
                                        <md-select v-model="unitMinAge" name="unitMinAge">
                                            <md-option value="Days">Days</md-option>
                                            <md-option value="Weeks">Weeks</md-option>
                                            <md-option value="Months">Months</md-option>
                                            <md-option value="Years">Years</md-option>
                                        </md-select>
                                    </md-field>
                                </div>
                                <div class="md-layout-item md-size-15">
                                    <md-field>
                                        <label for="maximumAge">Maximum age</label>
                                        <md-input v-model="maximumAge" type="number" name="maximumAge" required></md-input>
                                    </md-field>
                                </div>
                                <div class="md-layout-item md-size-20">
                                    <md-field>
                                        <label for="unitMaxAge">Unit for maximum age</label>
                                        <md-select v-model="unitMaxAge" name="unitMaxAge" disabled>
                                            <md-option value="Days">Days</md-option>
                                            <md-option value="Weeks">Weeks</md-option>
                                            <md-option value="Months">Months</md-option>
                                            <md-option value="Years">Years</md-option>
                                        </md-select>
                                    </md-field>
                                </div>

                                <!-- Gender -->
                                <div class="md-layout-item md-size-30">
                                    <md-field v-if="options['Gender'] != null">
                                        <label for="gender">Gender</label>
                                        <md-select v-model="gender" name="gender" required>
                                            <md-option v-for="item in options['Gender']" :key="item" :value="item">{{item}}</md-option>
                                        </md-select>
                                    </md-field>
                                </div>
                            </div>

                            <!-- Healthy Volunteers -->
                            <div class="md-layout-item md-size-50">
                                <div v-if="options['HealthyVolunteers'] != null">
                                    <label class="title" for="healthy">Accepts Healthy Volunteers</label>
                                    <div>
                                        <md-checkbox v-model="healthyVolunteers" class="md-primary" name="healthy" :value="true">Yes</md-checkbox>
                                    </div>
                                </div>
                            </div>

                            <!-- Eligibility Criteria -->
                            <div class="md-layout-item md-size-100 md-layout">
                                <div class="md-layout-item md-size-50 padding-left-0">
                                    <md-field v-for="(input, index) in eligibilityCriteria" :key="index" class="md-layout">
                                        <label>Eligibility Criteria</label>
                                        <md-textarea required v-model="input['value']" class="md-layout-item" style="margin-right: 25px" md-autogrow></md-textarea>
                                        <md-button class="md-just-icon md-round md-danger" v-if="index!=0" v-on:click="removeField('eligibilityCriteria', index);">
                                            <md-icon>remove</md-icon>
                                        </md-button>
                                    </md-field>
                                    <!-- <md-field v-for="(input, index) in eligibilityCriteria" :key="index" class="md-layout">
                                        <label>Eligibility Criteria</label>
                                        <md-input required v-model="input['value']" class="md-layout-item" style="margin-right: 25px"></md-input>
                                        <md-button class="md-just-icon md-round md-danger" v-if="index!=0" v-on:click="removeField('eligibilityCriteria', index);">
                                            <md-icon>remove</md-icon>
                                        </md-button>
                                    </md-field> -->
                                </div>
                                <md-button class="md-just-icon md-round" v-on:click="addField('eligibilityCriteria');">
                                    <md-icon>add</md-icon>
                                </md-button>
                            </div>
                            
                            <h3 class="md-layout-item md-size-100 title">Target search:</h3>

                            <!-- Official title -->
                            <div class="md-layout-item md-size-50">
                                <div class="padding-left-0">
                                    <md-field>
                                        <label>Official title</label>
                                        <md-textarea required v-model="officialTitle" class="md-layout-item" style="margin-right: 25px" md-autogrow></md-textarea>
                                    </md-field>
                                </div>
                            </div>

                            <!-- Intervention / treatment -->
                            <div class="md-layout-item md-size-100 md-layout">
                                <div v-if="options['InterventionName'] != null" class="md-layout-item md-size-50">
                                    <div v-for="(input, index) in interventionName" :key="index" class="md-layout">
                                        <md-autocomplete required v-model="input['value']" :md-options="options['InterventionName']" :md-fuzzy-search="false" class="md-layout-item" style="margin-right: 25px">
                                            <label>Intervention / treatment</label>
                                        </md-autocomplete>
                                        <md-button class="md-just-icon md-round md-danger" v-if="index!=0" v-on:click="removeField('interventionName', index);">
                                            <md-icon>remove</md-icon>
                                        </md-button>
                                    </div>
                                </div>
                                <md-button class="md-just-icon md-round" v-on:click="addField('interventionName');">
                                    <md-icon>add</md-icon>
                                </md-button>
                            </div>

                            <!-- Organisation Full Name -->
                            <div class="md-layout-item md-size-75">
                                <md-autocomplete required v-model="orgFullName" :md-options="options['OrgFullName']" class="md-layout-item" style="margin-right: 25px">
                                    <label>Organisation Full Name</label>
                                </md-autocomplete>
                            </div>

                            <!-- Organisation class -->
                            <div class="md-layout md-layout-item md-size-25">
                                <md-field v-if="options['OrgClass'] != null">
                                    <label>Org class</label>
                                    <md-select v-model="orgClass" required>
                                        <md-option v-for="item in options['OrgClass']" :key="item" :value="item">{{item}}</md-option>
                                    </md-select>
                                </md-field>
                            </div>

                            <!-- Lead Sponsor Class -->
                            <div class="md-layout-item md-size-100 md-layout">
                                <div v-if="options['LeadSponsorClass'] != null" class="md-layout-item md-size-50">
                                    <div v-for="(input, index) in leadSponsorClass" :key="index" class="md-layout">
                                        <md-autocomplete required v-model="input['value']" :md-options="options['LeadSponsorClass']" :md-fuzzy-search="false" class="md-layout-item" style="margin-right: 25px">
                                            <label>Lead Sponsor Class</label>
                                        </md-autocomplete>
                                        <md-button class="md-just-icon md-round md-danger" v-if="index!=0" v-on:click="removeField('leadSponsorClass', index);">
                                            <md-icon>remove</md-icon>
                                        </md-button>
                                    </div>
                                </div>
                                <md-button class="md-just-icon md-round" v-on:click="addField('leadSponsorClass');">
                                    <md-icon>add</md-icon>
                                </md-button>
                            </div>

                            <h3 class="md-layout-item md-size-100 title">Locations:</h3>

                            <!-- Locations -->
                            <div class="md-layout-item md-size-100 md-layout">
                                <div v-if="options['Locations_mapping'] != null" class="md-layout-item md-size-95">
                                    <div v-for="(input, index) in locations" :key="index" class="md-layout">
                                        <div class="md-layout-item md-size-50 padding-left-0">
                                            <md-field >
                                                <label for="locationCountry">Country</label>
                                                <md-select required v-model="input['locationCountry']" name="locationCountry">
                                                    <md-option v-for="(value, name) in options['Locations_mapping']" :key="name" :value="name">{{name}}</md-option>
                                                </md-select>
                                            </md-field>
                                        </div>
                                        <div class="md-layout-item md-size-50">
                                            <md-field v-if="input['locationCountry'] != ''">
                                                <label for="locationState">State</label>
                                                <md-select required v-model="input['locationState']" name="locationState" >
                                                    <md-option v-for="(value, name) in options['Locations_mapping'][input['locationCountry']]" :key="name" :value="name">
                                                        {{name}}
                                                    </md-option>
                                                </md-select>
                                            </md-field>
                                        </div>
                                        <div class="md-layout-item md-size-50 padding-left-0">
                                            <md-field v-if="input['locationState'] != ''">
                                                <label for="locationCity">City</label>
                                                <md-select required v-model="input['locationCity']" name="locationCity" >
                                                    <md-option v-for="(value, name) in options['Locations_mapping'][input['locationCountry']][input['locationState']]"
                                                    :key="name" :value="name">
                                                        {{name}}
                                                    </md-option>
                                                </md-select>
                                                
                                            </md-field>
                                        </div>
                                        <div class="md-layout-item md-size-50">
                                            <div v-if="input['locationCity'] != ''">
                                                <md-input type="hidden" v-model="input['locationPopulationDensity']"
                                                :value="options['Locations_mapping'][input['locationCountry']][input['locationState']][input['locationCity']]"></md-input>
                                                <!-- <label for="locationFacility">Facility</label> -->
                                                <md-autocomplete required v-model="input['locationFacility']" name="locationFacility"
                                                :md-options="options['Locations_mapping'][input['locationCountry']][input['locationState']][input['locationCity']].facilities">
                                                    <label>Facility</label>
                                                </md-autocomplete>
                                            </div>
                                        </div>
                                        <md-button class="md-just-icon md-round md-danger" v-if="index!=0" v-on:click="removeField('locations', index);">
                                            <md-icon>remove</md-icon>
                                        </md-button>
                                    </div>
                                </div>
                                <md-button class="md-just-icon md-round" v-on:click="addField('locations');">
                                    <md-icon>add</md-icon>
                                </md-button>
                            </div>

                            <h3 class="md-layout-item md-size-100 title">Additional Criteria</h3>

                            <!-- Phase -->
                            <div class="md-layout-item md-size-50">
                                <div v-if="options['Phase'] != null">
                                    <label class="title" for="phase">Phase (Choose at least one in three options)*</label>
                                    <div>
                                        <md-checkbox v-model="phase" class="md-primary" name="phase" value="Phase 1">Phase 1</md-checkbox>
                                        <md-checkbox v-model="phase" class="md-primary" name="phase" value="Phase 2">Phase 2</md-checkbox>
                                        <md-checkbox v-model="phase" class="md-primary" name="phase" value="Phase 3">Phase 3</md-checkbox>
                                    </div>
                                </div>
                            </div>

                            <!-- isFDARegulatedDevice -->
                            <div class="md-layout-item md-size-25">
                                <div v-if="options['IsFDARegulatedDevice'] != null">
                                    <label class="title" for="isFDARegulatedDevice">Is FDA Regulated Device?</label>
                                    <div>
                                        <md-checkbox v-model="isFDARegulatedDevice" class="md-primary" name="isFDARegulatedDevice" :value="true">Yes</md-checkbox>
                                    </div>
                                </div>
                            </div>

                            <!-- isFDARegulatedDrug -->
                            <div class="md-layout-item md-size-25">
                                <div v-if="options['IsFDARegulatedDrug'] != null">
                                    <label class="title" for="isFDARegulatedDrug">Is FDA Regulated Drug?</label>
                                    <div>
                                        <md-checkbox v-model="isFDARegulatedDrug" class="md-primary" name="isFDARegulatedDrug" :value="true">Yes</md-checkbox>
                                    </div>
                                </div>
                            </div>

                            <!-- Collaborator Class -->
                            <div class="md-layout-item md-size-50">
                                <div v-if="options['CollaboratorClass'] != null">
                                    <md-field>
                                        <label for="collaboratorClass">Collaborator Class</label>
                                        <md-select v-model="collaboratorClass" name="collaboratorClass" id="collaboratorClass" multiple required>
                                            <md-option v-for="item in options['CollaboratorClass']" :key="item" :value="item">{{item}}</md-option>
                                        </md-select>
                                    </md-field>
                                </div>
                            </div>

                            <!-- Start date -->
                            <div class="md-layout-item md-size-50">
                                <div v-if="options['StartDate'] != null">
                                    <md-datepicker v-model="startDate" required>
                                        <label>Select start date *</label>
                                    </md-datepicker>
                                </div>
                            </div>

                            <!-- Design Primary Purpose -->
                            <div class="md-layout-item md-size-33">
                                <div v-if="options['DesignPrimaryPurpose'] != null">
                                    <md-field>
                                        <label for="designPrimaryPurpose">Design Primary Purpose</label>
                                        <md-select v-model="designPrimaryPurpose" name="designPrimaryPurpose" id="designPrimaryPurpose" required>
                                            <md-option v-for="item in options['DesignPrimaryPurpose']" :key="item" :value="item">{{item}}</md-option>
                                        </md-select>
                                    </md-field>
                                </div>
                            </div>

                            <!-- Enrollment Count -->
                            <div class="md-layout-item md-size-33">
                                <div v-if="options['EnrollmentCount'] != null">
                                    <md-field>
                                        <label>Enrollment Count</label>
                                        <md-input v-model="enrollmentCount" type="number" required></md-input>
                                    </md-field>
                                </div>
                            </div>

                            <!-- Enrollment Type -->
                            <div class="md-layout-item md-size-33">
                                <div v-if="options['EnrollmentType'] != null">
                                    <md-field>
                                        <label for="enrollmentType">Enrollment Type</label>
                                        <md-select v-model="enrollmentType" name="enrollmentType" id="enrollmentType" required>
                                            <md-option v-for="item in options['EnrollmentType']" :key="item" :value="item">{{item}}</md-option>
                                        </md-select>
                                    </md-field>
                                </div>
                            </div>
                        </div>
                        
                        <div class="md-layout-item md-size-100">
                            <md-button class="md-default" style="margin-bottom: 30px" v-on:click="predict()">Predict</md-button>
                        </div>
                        <md-snackbar md-position="left" :md-duration="Infinity" :md-active.sync="showSnackbar" md-persistent>
                            <div>
                                <p class="text-danger">This form is having these following errors:</p>
                                <ul>
                                    <li v-for="item in errors" :key="item">{{ item }}</li>
                                </ul>


                            </div>
                            <md-button class="md-primary" @click="showSnackbar = false">Close</md-button>
                        </md-snackbar>
                    </div>
                    
                </div>
            </div>
            <div class="section text-center">
                <div class="container">
                    <!-- <p>{{ inputs }}</p>
                    <p>{{ inputs['Condition'] }}</p> -->
                    
                </div>
            </div>
        </div>

        <div class="vld-parent">
            <loading :active.sync="isLoading" 
            :can-cancel="true"
            :is-full-page="fullPage"></loading>
        </div>

        <md-dialog :md-active.sync="showDialog">
            <md-dialog-title>Estimated duration for this study</md-dialog-title>
            <md-dialog-content>
                <div v-if="prediction != -1">
                    <div class="text-center">
                        <h3 class="md-display-2 title text-success">{{predictionResult}} months</h3><!-- ( +/- {{deviation}}) -->
                        <img src="http://85.214.24.111:1800/sigmoid_img" />
                    </div>
                </div>
            </md-dialog-content>
            <md-dialog-actions>
                <md-button class="md-primary" @click="showDialog = false">Close</md-button>
            </md-dialog-actions>
        </md-dialog>
    </div>
</template>

<script>
import BasicElements from "./components/BasicElementsSection";
import Vue from 'vue';
import moment from 'moment';
import Loading from 'vue-loading-overlay';
import 'vue-loading-overlay/dist/vue-loading.css';

export default {
    components: {
        Loading
        // BasicElements
    },
    bodyClass: "landing-page",
    props: {
        header: {
            type: String,
            default: require("@/assets/img/clinicalBackground.jpg")
        },
        teamImg1: {
            type: String,
            default: require("@/assets/img/faces/caro.jpg")
        },
        teamImg2: {
            type: String,
            default: require("@/assets/img/faces/GH.png")
        },
        teamImg3: {
            type: String,
            default: require("@/assets/img/faces/Luka.jpg")
        },
        teamImg4: {
            type: String,
            default: require("@/assets/img/faces/stefan.jpg")
        },
        teamImg5: {
            type: String,
            default: require("@/assets/img/faces/weiyi-new.png")
        }
    },
    data() {
        let dateFormat = this.$material.locale.dateFormat || 'yyyy-MM-dd'
        let now = new Date()

        return {
            errors: [],
            showDialog: false,
            showSnackbar: false,
            isLoading: false,
            fullPage: true,
            options: {},

            multipleAnswersFields: {
                // ArmGroupLabel: 'armGroupLabel',
                Condition: 'condition',
                ConditionAncestorTerm: 'conditionAncestorTerm',
                ConditionBrowseBranchAbbrev: 'conditionBrowseBranchAbbrev',
                ConditionMeshId: 'conditionMeshId',
                InterventionName: 'interventionName',
                LeadSponsorClass: 'leadSponsorClass',
                EligibilityCriteria: 'eligibilityCriteria',
                //'locations',
                Phase: 'phase'
            },
            
            // All features will go here
            // armGroupLabel: [{value: ""}],                   // done
            collaboratorClass: [],                          // done
            condition: [{value: ""}],                       // done
            conditionAncestorTerm: [{value: ""}],           // done
            conditionBrowseBranchAbbrev: [{value: ""}],     // done
            conditionMeshId: [{value: ""}],                 // done
            designPrimaryPurpose: "None",                   // done
            eligibilityCriteria: [{value: ""}],
            enrollmentCount: 1,                             // done
            enrollmentType: "None",                         // done
            gender: "All",                                  // done
            healthyVolunteers: false,                       // done
            interventionName: [{value: ""}],                // done
            isFDARegulatedDevice: false,                    // done
            isFDARegulatedDrug: false,                      // done
            leadSponsorClass: [{value: ""}],                // done
            locations: [
                {
                    locationCity: "",                       // done
                    locationCountry: "",                    // done
                    locationFacility: "",                   
                    locationPopulationDensity: 0,
                    locationState: ""                       // done
                }
            ],
            maximumAge: "",
            unitMaxAge: "Years",
            minimumAge: "",
            unitMinAge: "Years",
            orgClass: "",                                    // done
            orgFullName: "",                                 // done
            phase: [{value: ""}],                            // done
            startDate: now,                                 // done
            stdAge: [],                                       // done
            officialTitle: "",

            predictionResult: -1,
            deviation: -1,
            img: null
        };
    },
    methods: {
        addField(obj) {
            const newObj = {
                value: ""
            };
            const newLocationObj = {
                locationCity: "",
                locationCountry: "",
                locationFacility: "",
                locationPopulationDensity: 0,
                locationState: ""
            };

            if (obj == 'locations') {
                this[obj].push(newLocationObj)
            }
            else {
                this[obj].push(newObj);
            }

        },
        removeField(obj, index) {
            this[obj].splice(index, 1)
        },
        predict() {
            // check missing field when fill in params variable and check null
            let params = {
                "OrgClass": this.orgClass, 
                "Condition": [], 
                "LeadSponsorClass": [], 
                "CollaboratorClass": this.collaboratorClass, 
                "EligibilityCriteria": [],
                "EnrollmentType": this.enrollmentType, 
                "EnrollmentCount": parseInt(this.enrollmentCount), 
                "Phase": [], 
                "HealthyVolunteers": "", 
                "Gender": this.gender, 
                "StdAge": [], 
                "LocationFacility": [], 
                "LocationCity": [], 
                "LocationCountry": [], 
                "IsFDARegulatedDrug": "", 
                "IsFDARegulatedDevice": "",
                "InterventionName": [], 
                "StartDate": "", // May 2020 
                "DesignPrimaryPurpose": this.designPrimaryPurpose, 
                "MaximumAge": this.maximumAge.toString() + " " + this.unitMaxAge, 
                "MinimumAge": this.minimumAge.toString() + " " + this.unitMinAge, 
                "OrgFullName": this.orgFullName, 
                "ConditionAncestorTerm": [], 
                "ConditionBrowseBranchAbbrev": [],
                "ConditionMeshId": [], 
                "LocationState": [],
                "LocationPopulationDensity": [],
                "OfficialTitle": this.officialTitle,
                "BriefSummary": this.officialTitle
            }

            const newParams = {
                "ArmGroupLabel": [],
                "LeadSponsorName": [],
                "InterventionType": [],
                "CollaboratorName": [],
                "ArmGroupType" : [],
                "BaselineDenomCountGroupId": [],
                "BaselineMeasureDispersionType" : [],
                "DesignAllocation" : "Randomized",
                "DesignInterventionModel" : "Sequential Assignment",
                "Keyword" : [],
                "EventsTimeFrame": "Until 52 weeks (26+26) weeks",
                "FlowDropWithdrawType": [],
                "FlowGroupDescription": [],
                "FlowGroupTitle": [],
                "FlowMilestoneType": [],
                "FlowPeriodTitle": [],
                "FlowRecruitmentDetails": "",
                "ArmGroupDescription": [],
                "ArmGroupInterventionName": [],
                "BaselineCategoryTitle": [],
                "BaselineClassTitle": [],
                "BaselineGroupDescription": [],
                "BaselineGroupTitle": [],
                "BaselineMeasureTitle": [],
                "BaselineMeasureUnitOfMeasure": [],
                "LocationZip": []
            }

            // Handle "multiple options" fields
            for (const [key, value] of Object.entries(this.multipleAnswersFields)) {
                for (let i=0; i < this[value].length; i++) {
                    params[key].push(this[value][i].value)
                }
                // params[key] = this[value]
            }

            // HealthyVolunteers
            this.healthyVolunteers == true ? params['HealthyVolunteers'] = 'Accepts Healthy Volunteers' : params['HealthyVolunteers'] = 'No';

            // IsFDARegulatedDrug
            this.isFDARegulatedDrug == true ? params['IsFDARegulatedDrug'] = 'Yes' : params['IsFDARegulatedDrug'] = 'No';

            // HealthyVolunteers
            this.isFDARegulatedDevice == true ? params['IsFDARegulatedDevice'] = 'Yes' : params['IsFDARegulatedDevice'] = 'No';
            
            // Convert start date => Month Year format
            params['StartDate'] = moment(this.startDate).format('MMMM YYYY')

            // copy from min age and max age => stdAge
            if (this.unitMinAge != 'Years') {
                this.stdAge.push("Child");

                // Assumming unit max age is always "years"
                if (this.maximumAge >= 65) {
                    this.stdAge.push("Adult", "Older Adult");
                }
                else if (this.maximumAge >= 18) {
                    this.stdAge.push("Adult")
                }
            }
            else {

                if (this.minimumAge <= 17) {
                    this.stdAge.push("Child");

                    if (this.maximumAge >= 65) {
                        this.stdAge.push("Adult", "Older Adult");
                    }
                    else if (this.maximumAge >= 18) {
                        this.stdAge.push("Adult")
                    } 
                }
                else if (this.minimumAge <= 64) {
                    this.stdAge.push("Adult");

                    if (this.maximumAge >= 65) {
                        this.stdAge.push("Older Adult")
                    }
                }
                else {
                    this.stdAge.push("Older Adult");
                }
            }
            params["StdAge"] = this.stdAge

            // handle locations
            this.locations.forEach(obj => {
                params['LocationCountry'].push(obj.locationCountry);
                params['LocationCity'].push(obj.locationCity);
                params['LocationFacility'].push(obj.locationFacility);
                params['LocationState'].push(obj.locationState);
                params['LocationPopulationDensity'].push(obj.locationPopulationDensity);
            })

            // Check null
            let checkNullResult = this.checkNull(params)

            if (checkNullResult == true) {
                for (const [key, value] of Object.entries(newParams)) {
                    params[key] = value
                }

                console.log(params)

                // Trigger block user (loading process)
                this.isLoading = true;
                this.predictionResult = -1
                this.$axios
                    .post('http://85.214.24.111:1800/prediction?type=LGBM', params=params)
                    .then(response => {
                        // Save all options
                        let r = response.data
                        this.predictionResult = Math.round(r.prediction)
                        // this.deviation = Math.round(r.result.test_results['MAE'])
                        this.isLoading = false
                        this.showDialog = true

                    },
                    (error) => { console.log(error) })
                    
            }
            else {
                console.log("Check null fail")
            }
        },

        checkNull(params) {
            // let result = []
            this.errors = []

            for (const [key, value] of Object.entries(params)) {
                if ((typeof value) == 'string'){
                    if (value == "") {
                        this.errors.push(key + " is empty")
                        console.log(key + " is empty")
                    }
                    else if (value == " Years") { // For MinimumAge and MaximumAge
                        this.errors.push(key + " is empty")
                        console.log(key + " is empty")
                    }
                }

                else if ((typeof value) == 'object') {
                    if (Array.isArray(value) == true) {
                        if (value.length == 0) {
                            this.errors.push(key + " is empty")
                            console.log(key + " is empty")
                        }
                        else if (value.length == 1) {
                            if (value == "") {
                                this.errors.push(key + " is empty")
                                console.log(key + " is empty")
                            }
                        }
                        else {
                            console.log(typeof value[0])
                        }
                    }
                }
                
            }

            if (this.errors.length > 0) {
                this.showSnackbar = true
                return false
            }
            else {
                return true
            }
        }
    },
    mounted () {
        this.$axios
            .get('http://85.214.24.111:1800/allvalues') // http://85.214.24.111:1234/ => Production server 1; http://85.214.24.111:1800/ => Production server 2; http://134.155.110.156:1234 => Development server; 
            .then(response => {
                // Save all options
                this.options = response.data.result;
            })
    },
    computed: {
        headerStyle() {
            return {
                backgroundImage: `url(${this.header})`
            };
        }
        // ,
        // dateFormat: {
        //     get () {
        //         return this.$material.locale.dateFormat
        //     },
        //     set () {
        //         this.$material.locale.dateFormat = "dd-mm-yyyy"
        //     }
        // }
    }
}
    
</script>

<style lang="scss">

.padding-left-0{
    padding-left: 0 !important;
}

.md-ripple > span.md-list-item-text {
    position: unset !important;
}

.input_components > div{
    margin-bottom: 30px !important;
}
.md-snackbar.md-theme-default {
    background-color: #ffffff !important;
    color: #323232 !important;
}
.md-snackbar {
    max-height: unset !important;
}
</style>