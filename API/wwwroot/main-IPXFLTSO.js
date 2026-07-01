var zk=Object.defineProperty,$k=Object.defineProperties;var Gk=Object.getOwnPropertyDescriptors;var ky=Object.getOwnPropertySymbols;var Wk=Object.prototype.hasOwnProperty,qk=Object.prototype.propertyIsEnumerable;var My=(t,n,e)=>n in t?zk(t,n,{enumerable:!0,configurable:!0,writable:!0,value:e}):t[n]=e,b=(t,n)=>{for(var e in n||={})Wk.call(n,e)&&My(t,e,n[e]);if(ky)for(var e of ky(n))qk.call(n,e)&&My(t,e,n[e]);return t},Z=(t,n)=>$k(t,Gk(n));var Ss=(t=>typeof require<"u"?require:typeof Proxy<"u"?new Proxy(t,{get:(n,e)=>(typeof require<"u"?require:n)[e]}):t)(function(t){if(typeof require<"u")return require.apply(this,arguments);throw Error('Dynamic require of "'+t+'" is not supported')});var en=null,Cd=!1,ip=1,Yk=null,Et=Symbol("SIGNAL");function te(t){let n=en;return en=t,n}function wd(){return en}var Rr={version:0,lastCleanEpoch:0,dirty:!1,producers:void 0,producersTail:void 0,consumers:void 0,consumersTail:void 0,recomputing:!1,consumerAllowSignalWrites:!1,consumerIsAlwaysLive:!1,kind:"unknown",producerMustRecompute:()=>!1,producerRecomputeValue:()=>{},consumerMarkedDirty:()=>{},consumerOnSignalRead:()=>{}};function Or(t){if(Cd)throw new Error("");if(en===null)return;en.consumerOnSignalRead(t);let n=en.producersTail;if(n!==void 0&&n.producer===t)return;let e,i=en.recomputing;if(i&&(e=n!==void 0?n.nextProducer:en.producers,e!==void 0&&e.producer===t)){en.producersTail=e,e.lastReadVersion=t.version;return}let r=t.consumersTail;if(r!==void 0&&r.consumer===en&&(!i||Qk(r,en)))return;let o=Ms(en),s={producer:t,consumer:en,nextProducer:e,prevConsumer:r,lastReadVersion:t.version,nextConsumer:void 0};en.producersTail=s,n!==void 0?n.nextProducer=s:en.producers=s,o&&Oy(t,s)}function Ty(){ip++}function Eo(t){if(!(Ms(t)&&!t.dirty)&&!(!t.dirty&&t.lastCleanEpoch===ip)){if(!t.producerMustRecompute(t)&&!ks(t)){Is(t);return}t.producerRecomputeValue(t),Is(t)}}function rp(t){if(t.consumers===void 0)return;let n=Cd;Cd=!0;try{for(let e=t.consumers;e!==void 0;e=e.nextConsumer){let i=e.consumer;i.dirty||Zk(i)}}finally{Cd=n}}function op(){return en?.consumerAllowSignalWrites!==!1}function Zk(t){t.dirty=!0,rp(t),t.consumerMarkedDirty?.(t)}function Is(t){t.dirty=!1,t.lastCleanEpoch=ip}function rr(t){return t&&Ay(t),te(t)}function Ay(t){t.producersTail=void 0,t.recomputing=!0}function Nr(t,n){te(n),t&&Ry(t)}function Ry(t){t.recomputing=!1;let n=t.producersTail,e=n!==void 0?n.nextProducer:t.producers;if(e!==void 0){if(Ms(t))do e=sp(e);while(e!==void 0);n!==void 0?n.nextProducer=void 0:t.producers=void 0}}function ks(t){for(let n=t.producers;n!==void 0;n=n.nextProducer){let e=n.producer,i=n.lastReadVersion;if(i!==e.version||(Eo(e),i!==e.version))return!0}return!1}function Pr(t){if(Ms(t)){let n=t.producers;for(;n!==void 0;)n=sp(n)}t.producers=void 0,t.producersTail=void 0,t.consumers=void 0,t.consumersTail=void 0}function Oy(t,n){let e=t.consumersTail,i=Ms(t);if(e!==void 0?(n.nextConsumer=e.nextConsumer,e.nextConsumer=n):(n.nextConsumer=void 0,t.consumers=n),n.prevConsumer=e,t.consumersTail=n,!i)for(let r=t.producers;r!==void 0;r=r.nextProducer)Oy(r.producer,r)}function sp(t){let n=t.producer,e=t.nextProducer,i=t.nextConsumer,r=t.prevConsumer;if(t.nextConsumer=void 0,t.prevConsumer=void 0,i!==void 0?i.prevConsumer=r:n.consumersTail=r,r!==void 0)r.nextConsumer=i;else if(n.consumers=i,!Ms(n)){let o=n.producers;for(;o!==void 0;)o=sp(o)}return e}function Ms(t){return t.consumerIsAlwaysLive||t.consumers!==void 0}function tc(t){Yk?.(t)}function Qk(t,n){let e=n.producersTail;if(e!==void 0){let i=n.producers;do{if(i===t)return!0;if(i===e)break;i=i.nextProducer}while(i!==void 0)}return!1}function nc(t,n){return Object.is(t,n)}function ic(t,n){let e=Object.create(Xk);e.computation=t,n!==void 0&&(e.equal=n);let i=()=>{if(Eo(e),Or(e),e.value===ki)throw e.error;return e.value};return i[Et]=e,tc(e),i}var wo=Symbol("UNSET"),Do=Symbol("COMPUTING"),ki=Symbol("ERRORED"),Xk=Z(b({},Rr),{value:wo,dirty:!0,error:null,equal:nc,kind:"computed",producerMustRecompute(t){return t.value===wo||t.value===Do},producerRecomputeValue(t){if(t.value===Do)throw new Error("");let n=t.value;t.value=Do;let e=rr(t),i,r=!1;try{i=t.computation(),te(null),r=n!==wo&&n!==ki&&i!==ki&&t.equal(n,i)}catch(o){i=ki,t.error=o}finally{Nr(t,e)}if(r){t.value=n;return}t.value=i,t.version++}});function Kk(){throw new Error}var Ny=Kk;function Py(t){Ny(t)}function ap(t){Ny=t}var Jk=null;function cp(t,n){let e=Object.create(rc);e.value=t,n!==void 0&&(e.equal=n);let i=()=>Fy(e);return i[Et]=e,tc(e),[i,s=>So(e,s),s=>Dd(e,s)]}function Fy(t){return Or(t),t.value}function So(t,n){op()||Py(t),t.equal(t.value,n)||(t.value=n,eM(t))}function Dd(t,n){op()||Py(t),So(t,n(t.value))}var rc=Z(b({},Rr),{equal:nc,value:void 0,kind:"signal"});function eM(t){t.version++,Ty(),rp(t),Jk?.(t)}var lp=Z(b({},Rr),{consumerIsAlwaysLive:!0,consumerAllowSignalWrites:!0,dirty:!0,kind:"effect"});function dp(t){if(t.dirty=!1,t.version>0&&!ks(t))return;t.version++;let n=rr(t);try{t.cleanup(),t.fn()}finally{Nr(t,n)}}function ue(t){return typeof t=="function"}function Ts(t){let e=t(i=>{Error.call(i),i.stack=new Error().stack});return e.prototype=Object.create(Error.prototype),e.prototype.constructor=e,e}var Ed=Ts(t=>function(e){t(this),this.message=e?`${e.length} errors occurred during unsubscription:
${e.map((i,r)=>`${r+1}) ${i.toString()}`).join(`
  `)}`:"",this.name="UnsubscriptionError",this.errors=e});function Io(t,n){if(t){let e=t.indexOf(n);0<=e&&t.splice(e,1)}}var ge=class t{constructor(n){this.initialTeardown=n,this.closed=!1,this._parentage=null,this._finalizers=null}unsubscribe(){let n;if(!this.closed){this.closed=!0;let{_parentage:e}=this;if(e)if(this._parentage=null,Array.isArray(e))for(let o of e)o.remove(this);else e.remove(this);let{initialTeardown:i}=this;if(ue(i))try{i()}catch(o){n=o instanceof Ed?o.errors:[o]}let{_finalizers:r}=this;if(r){this._finalizers=null;for(let o of r)try{Ly(o)}catch(s){n=n??[],s instanceof Ed?n=[...n,...s.errors]:n.push(s)}}if(n)throw new Ed(n)}}add(n){var e;if(n&&n!==this)if(this.closed)Ly(n);else{if(n instanceof t){if(n.closed||n._hasParent(this))return;n._addParent(this)}(this._finalizers=(e=this._finalizers)!==null&&e!==void 0?e:[]).push(n)}}_hasParent(n){let{_parentage:e}=this;return e===n||Array.isArray(e)&&e.includes(n)}_addParent(n){let{_parentage:e}=this;this._parentage=Array.isArray(e)?(e.push(n),e):e?[e,n]:n}_removeParent(n){let{_parentage:e}=this;e===n?this._parentage=null:Array.isArray(e)&&Io(e,n)}remove(n){let{_finalizers:e}=this;e&&Io(e,n),n instanceof t&&n._removeParent(this)}};ge.EMPTY=(()=>{let t=new ge;return t.closed=!0,t})();var up=ge.EMPTY;function Sd(t){return t instanceof ge||t&&"closed"in t&&ue(t.remove)&&ue(t.add)&&ue(t.unsubscribe)}function Ly(t){ue(t)?t():t.unsubscribe()}var oi={onUnhandledError:null,onStoppedNotification:null,Promise:void 0,useDeprecatedSynchronousErrorHandling:!1,useDeprecatedNextContext:!1};var As={setTimeout(t,n,...e){let{delegate:i}=As;return i?.setTimeout?i.setTimeout(t,n,...e):setTimeout(t,n,...e)},clearTimeout(t){let{delegate:n}=As;return(n?.clearTimeout||clearTimeout)(t)},delegate:void 0};function Id(t){As.setTimeout(()=>{let{onUnhandledError:n}=oi;if(n)n(t);else throw t})}function oc(){}var jy=mp("C",void 0,void 0);function By(t){return mp("E",void 0,t)}function Vy(t){return mp("N",t,void 0)}function mp(t,n,e){return{kind:t,value:n,error:e}}var ko=null;function Rs(t){if(oi.useDeprecatedSynchronousErrorHandling){let n=!ko;if(n&&(ko={errorThrown:!1,error:null}),t(),n){let{errorThrown:e,error:i}=ko;if(ko=null,e)throw i}}else t()}function Hy(t){oi.useDeprecatedSynchronousErrorHandling&&ko&&(ko.errorThrown=!0,ko.error=t)}var Mo=class extends ge{constructor(n){super(),this.isStopped=!1,n?(this.destination=n,Sd(n)&&n.add(this)):this.destination=iM}static create(n,e,i){return new si(n,e,i)}next(n){this.isStopped?hp(Vy(n),this):this._next(n)}error(n){this.isStopped?hp(By(n),this):(this.isStopped=!0,this._error(n))}complete(){this.isStopped?hp(jy,this):(this.isStopped=!0,this._complete())}unsubscribe(){this.closed||(this.isStopped=!0,super.unsubscribe(),this.destination=null)}_next(n){this.destination.next(n)}_error(n){try{this.destination.error(n)}finally{this.unsubscribe()}}_complete(){try{this.destination.complete()}finally{this.unsubscribe()}}},tM=Function.prototype.bind;function fp(t,n){return tM.call(t,n)}var pp=class{constructor(n){this.partialObserver=n}next(n){let{partialObserver:e}=this;if(e.next)try{e.next(n)}catch(i){kd(i)}}error(n){let{partialObserver:e}=this;if(e.error)try{e.error(n)}catch(i){kd(i)}else kd(n)}complete(){let{partialObserver:n}=this;if(n.complete)try{n.complete()}catch(e){kd(e)}}},si=class extends Mo{constructor(n,e,i){super();let r;if(ue(n)||!n)r={next:n??void 0,error:e??void 0,complete:i??void 0};else{let o;this&&oi.useDeprecatedNextContext?(o=Object.create(n),o.unsubscribe=()=>this.unsubscribe(),r={next:n.next&&fp(n.next,o),error:n.error&&fp(n.error,o),complete:n.complete&&fp(n.complete,o)}):r=n}this.destination=new pp(r)}};function kd(t){oi.useDeprecatedSynchronousErrorHandling?Hy(t):Id(t)}function nM(t){throw t}function hp(t,n){let{onStoppedNotification:e}=oi;e&&As.setTimeout(()=>e(t,n))}var iM={closed:!0,next:oc,error:nM,complete:oc};var Os=typeof Symbol=="function"&&Symbol.observable||"@@observable";function On(t){return t}function gp(...t){return _p(t)}function _p(t){return t.length===0?On:t.length===1?t[0]:function(e){return t.reduce((i,r)=>r(i),e)}}var ce=(()=>{class t{constructor(e){e&&(this._subscribe=e)}lift(e){let i=new t;return i.source=this,i.operator=e,i}subscribe(e,i,r){let o=oM(e)?e:new si(e,i,r);return Rs(()=>{let{operator:s,source:a}=this;o.add(s?s.call(o,a):a?this._subscribe(o):this._trySubscribe(o))}),o}_trySubscribe(e){try{return this._subscribe(e)}catch(i){e.error(i)}}forEach(e,i){return i=Uy(i),new i((r,o)=>{let s=new si({next:a=>{try{e(a)}catch(c){o(c),s.unsubscribe()}},error:o,complete:r});this.subscribe(s)})}_subscribe(e){var i;return(i=this.source)===null||i===void 0?void 0:i.subscribe(e)}[Os](){return this}pipe(...e){return _p(e)(this)}toPromise(e){return e=Uy(e),new e((i,r)=>{let o;this.subscribe(s=>o=s,s=>r(s),()=>i(o))})}}return t.create=n=>new t(n),t})();function Uy(t){var n;return(n=t??oi.Promise)!==null&&n!==void 0?n:Promise}function rM(t){return t&&ue(t.next)&&ue(t.error)&&ue(t.complete)}function oM(t){return t&&t instanceof Mo||rM(t)&&Sd(t)}function sM(t){return ue(t?.lift)}function he(t){return n=>{if(sM(n))return n.lift(function(e){try{return t(e,this)}catch(i){this.error(i)}});throw new TypeError("Unable to lift unknown Observable type")}}function _e(t,n,e,i,r){return new vp(t,n,e,i,r)}var vp=class extends Mo{constructor(n,e,i,r,o,s){super(n),this.onFinalize=o,this.shouldUnsubscribe=s,this._next=e?function(a){try{e(a)}catch(c){n.error(c)}}:super._next,this._error=r?function(a){try{r(a)}catch(c){n.error(c)}finally{this.unsubscribe()}}:super._error,this._complete=i?function(){try{i()}catch(a){n.error(a)}finally{this.unsubscribe()}}:super._complete}unsubscribe(){var n;if(!this.shouldUnsubscribe||this.shouldUnsubscribe()){let{closed:e}=this;super.unsubscribe(),!e&&((n=this.onFinalize)===null||n===void 0||n.call(this))}}};var zy=Ts(t=>function(){t(this),this.name="ObjectUnsubscribedError",this.message="object unsubscribed"});var I=(()=>{class t extends ce{constructor(){super(),this.closed=!1,this.currentObservers=null,this.observers=[],this.isStopped=!1,this.hasError=!1,this.thrownError=null}lift(e){let i=new Md(this,this);return i.operator=e,i}_throwIfClosed(){if(this.closed)throw new zy}next(e){Rs(()=>{if(this._throwIfClosed(),!this.isStopped){this.currentObservers||(this.currentObservers=Array.from(this.observers));for(let i of this.currentObservers)i.next(e)}})}error(e){Rs(()=>{if(this._throwIfClosed(),!this.isStopped){this.hasError=this.isStopped=!0,this.thrownError=e;let{observers:i}=this;for(;i.length;)i.shift().error(e)}})}complete(){Rs(()=>{if(this._throwIfClosed(),!this.isStopped){this.isStopped=!0;let{observers:e}=this;for(;e.length;)e.shift().complete()}})}unsubscribe(){this.isStopped=this.closed=!0,this.observers=this.currentObservers=null}get observed(){var e;return((e=this.observers)===null||e===void 0?void 0:e.length)>0}_trySubscribe(e){return this._throwIfClosed(),super._trySubscribe(e)}_subscribe(e){return this._throwIfClosed(),this._checkFinalizedStatuses(e),this._innerSubscribe(e)}_innerSubscribe(e){let{hasError:i,isStopped:r,observers:o}=this;return i||r?up:(this.currentObservers=null,o.push(e),new ge(()=>{this.currentObservers=null,Io(o,e)}))}_checkFinalizedStatuses(e){let{hasError:i,thrownError:r,isStopped:o}=this;i?e.error(r):o&&e.complete()}asObservable(){let e=new ce;return e.source=this,e}}return t.create=(n,e)=>new Md(n,e),t})(),Md=class extends I{constructor(n,e){super(),this.destination=n,this.source=e}next(n){var e,i;(i=(e=this.destination)===null||e===void 0?void 0:e.next)===null||i===void 0||i.call(e,n)}error(n){var e,i;(i=(e=this.destination)===null||e===void 0?void 0:e.error)===null||i===void 0||i.call(e,n)}complete(){var n,e;(e=(n=this.destination)===null||n===void 0?void 0:n.complete)===null||e===void 0||e.call(n)}_subscribe(n){var e,i;return(i=(e=this.source)===null||e===void 0?void 0:e.subscribe(n))!==null&&i!==void 0?i:up}};var Ft=class extends I{constructor(n){super(),this._value=n}get value(){return this.getValue()}_subscribe(n){let e=super._subscribe(n);return!e.closed&&n.next(this._value),e}getValue(){let{hasError:n,thrownError:e,_value:i}=this;if(n)throw e;return this._throwIfClosed(),i}next(n){super.next(this._value=n)}};var sc={now(){return(sc.delegate||Date).now()},delegate:void 0};var Mi=class extends I{constructor(n=1/0,e=1/0,i=sc){super(),this._bufferSize=n,this._windowTime=e,this._timestampProvider=i,this._buffer=[],this._infiniteTimeWindow=!0,this._infiniteTimeWindow=e===1/0,this._bufferSize=Math.max(1,n),this._windowTime=Math.max(1,e)}next(n){let{isStopped:e,_buffer:i,_infiniteTimeWindow:r,_timestampProvider:o,_windowTime:s}=this;e||(i.push(n),!r&&i.push(o.now()+s)),this._trimBuffer(),super.next(n)}_subscribe(n){this._throwIfClosed(),this._trimBuffer();let e=this._innerSubscribe(n),{_infiniteTimeWindow:i,_buffer:r}=this,o=r.slice();for(let s=0;s<o.length&&!n.closed;s+=i?1:2)n.next(o[s]);return this._checkFinalizedStatuses(n),e}_trimBuffer(){let{_bufferSize:n,_timestampProvider:e,_buffer:i,_infiniteTimeWindow:r}=this,o=(r?1:2)*n;if(n<1/0&&o<i.length&&i.splice(0,i.length-o),!r){let s=e.now(),a=0;for(let c=1;c<i.length&&i[c]<=s;c+=2)a=c;a&&i.splice(0,a+1)}}};var Td=class extends ge{constructor(n,e){super()}schedule(n,e=0){return this}};var ac={setInterval(t,n,...e){let{delegate:i}=ac;return i?.setInterval?i.setInterval(t,n,...e):setInterval(t,n,...e)},clearInterval(t){let{delegate:n}=ac;return(n?.clearInterval||clearInterval)(t)},delegate:void 0};var Ad=class extends Td{constructor(n,e){super(n,e),this.scheduler=n,this.work=e,this.pending=!1}schedule(n,e=0){var i;if(this.closed)return this;this.state=n;let r=this.id,o=this.scheduler;return r!=null&&(this.id=this.recycleAsyncId(o,r,e)),this.pending=!0,this.delay=e,this.id=(i=this.id)!==null&&i!==void 0?i:this.requestAsyncId(o,this.id,e),this}requestAsyncId(n,e,i=0){return ac.setInterval(n.flush.bind(n,this),i)}recycleAsyncId(n,e,i=0){if(i!=null&&this.delay===i&&this.pending===!1)return e;e!=null&&ac.clearInterval(e)}execute(n,e){if(this.closed)return new Error("executing a cancelled action");this.pending=!1;let i=this._execute(n,e);if(i)return i;this.pending===!1&&this.id!=null&&(this.id=this.recycleAsyncId(this.scheduler,this.id,null))}_execute(n,e){let i=!1,r;try{this.work(n)}catch(o){i=!0,r=o||new Error("Scheduled action threw falsy error")}if(i)return this.unsubscribe(),r}unsubscribe(){if(!this.closed){let{id:n,scheduler:e}=this,{actions:i}=e;this.work=this.state=this.scheduler=null,this.pending=!1,Io(i,this),n!=null&&(this.id=this.recycleAsyncId(e,n,null)),this.delay=null,super.unsubscribe()}}};var Ns=class t{constructor(n,e=t.now){this.schedulerActionCtor=n,this.now=e}schedule(n,e=0,i){return new this.schedulerActionCtor(this,n).schedule(i,e)}};Ns.now=sc.now;var Rd=class extends Ns{constructor(n,e=Ns.now){super(n,e),this.actions=[],this._active=!1}flush(n){let{actions:e}=this;if(this._active){e.push(n);return}let i;this._active=!0;do if(i=n.execute(n.state,n.delay))break;while(n=e.shift());if(this._active=!1,i){for(;n=e.shift();)n.unsubscribe();throw i}}};var cc=new Rd(Ad),$y=cc;var St=new ce(t=>t.complete());function Od(t){return t&&ue(t.schedule)}function bp(t){return t[t.length-1]}function Nd(t){return ue(bp(t))?t.pop():void 0}function Ti(t){return Od(bp(t))?t.pop():void 0}function Gy(t,n){return typeof bp(t)=="number"?t.pop():n}function qy(t,n,e,i){function r(o){return o instanceof e?o:new e(function(s){s(o)})}return new(e||(e=Promise))(function(o,s){function a(u){try{l(i.next(u))}catch(m){s(m)}}function c(u){try{l(i.throw(u))}catch(m){s(m)}}function l(u){u.done?o(u.value):r(u.value).then(a,c)}l((i=i.apply(t,n||[])).next())})}function Wy(t){var n=typeof Symbol=="function"&&Symbol.iterator,e=n&&t[n],i=0;if(e)return e.call(t);if(t&&typeof t.length=="number")return{next:function(){return t&&i>=t.length&&(t=void 0),{value:t&&t[i++],done:!t}}};throw new TypeError(n?"Object is not iterable.":"Symbol.iterator is not defined.")}function To(t){return this instanceof To?(this.v=t,this):new To(t)}function Yy(t,n,e){if(!Symbol.asyncIterator)throw new TypeError("Symbol.asyncIterator is not defined.");var i=e.apply(t,n||[]),r,o=[];return r=Object.create((typeof AsyncIterator=="function"?AsyncIterator:Object).prototype),a("next"),a("throw"),a("return",s),r[Symbol.asyncIterator]=function(){return this},r;function s(v){return function(x){return Promise.resolve(x).then(v,m)}}function a(v,x){i[v]&&(r[v]=function(F){return new Promise(function(j,Y){o.push([v,F,j,Y])>1||c(v,F)})},x&&(r[v]=x(r[v])))}function c(v,x){try{l(i[v](x))}catch(F){g(o[0][3],F)}}function l(v){v.value instanceof To?Promise.resolve(v.value.v).then(u,m):g(o[0][2],v)}function u(v){c("next",v)}function m(v){c("throw",v)}function g(v,x){v(x),o.shift(),o.length&&c(o[0][0],o[0][1])}}function Zy(t){if(!Symbol.asyncIterator)throw new TypeError("Symbol.asyncIterator is not defined.");var n=t[Symbol.asyncIterator],e;return n?n.call(t):(t=typeof Wy=="function"?Wy(t):t[Symbol.iterator](),e={},i("next"),i("throw"),i("return"),e[Symbol.asyncIterator]=function(){return this},e);function i(o){e[o]=t[o]&&function(s){return new Promise(function(a,c){s=t[o](s),r(a,c,s.done,s.value)})}}function r(o,s,a,c){Promise.resolve(c).then(function(l){o({value:l,done:a})},s)}}var Pd=t=>t&&typeof t.length=="number"&&typeof t!="function";function Fd(t){return ue(t?.then)}function Ld(t){return ue(t[Os])}function jd(t){return Symbol.asyncIterator&&ue(t?.[Symbol.asyncIterator])}function Bd(t){return new TypeError(`You provided ${t!==null&&typeof t=="object"?"an invalid object":`'${t}'`} where a stream was expected. You can provide an Observable, Promise, ReadableStream, Array, AsyncIterable, or Iterable.`)}function aM(){return typeof Symbol!="function"||!Symbol.iterator?"@@iterator":Symbol.iterator}var Vd=aM();function Hd(t){return ue(t?.[Vd])}function Ud(t){return Yy(this,arguments,function*(){let e=t.getReader();try{for(;;){let{value:i,done:r}=yield To(e.read());if(r)return yield To(void 0);yield yield To(i)}}finally{e.releaseLock()}})}function zd(t){return ue(t?.getReader)}function Ze(t){if(t instanceof ce)return t;if(t!=null){if(Ld(t))return cM(t);if(Pd(t))return lM(t);if(Fd(t))return dM(t);if(jd(t))return Qy(t);if(Hd(t))return uM(t);if(zd(t))return mM(t)}throw Bd(t)}function cM(t){return new ce(n=>{let e=t[Os]();if(ue(e.subscribe))return e.subscribe(n);throw new TypeError("Provided object does not correctly implement Symbol.observable")})}function lM(t){return new ce(n=>{for(let e=0;e<t.length&&!n.closed;e++)n.next(t[e]);n.complete()})}function dM(t){return new ce(n=>{t.then(e=>{n.closed||(n.next(e),n.complete())},e=>n.error(e)).then(null,Id)})}function uM(t){return new ce(n=>{for(let e of t)if(n.next(e),n.closed)return;n.complete()})}function Qy(t){return new ce(n=>{fM(t,n).catch(e=>n.error(e))})}function mM(t){return Qy(Ud(t))}function fM(t,n){var e,i,r,o;return qy(this,void 0,void 0,function*(){try{for(e=Zy(t);i=yield e.next(),!i.done;){let s=i.value;if(n.next(s),n.closed)return}}catch(s){r={error:s}}finally{try{i&&!i.done&&(o=e.return)&&(yield o.call(e))}finally{if(r)throw r.error}}n.complete()})}function wn(t,n,e,i=0,r=!1){let o=n.schedule(function(){e(),r?t.add(this.schedule(null,i)):this.unsubscribe()},i);if(t.add(o),!r)return o}function $d(t,n=0){return he((e,i)=>{e.subscribe(_e(i,r=>wn(i,t,()=>i.next(r),n),()=>wn(i,t,()=>i.complete(),n),r=>wn(i,t,()=>i.error(r),n)))})}function Gd(t,n=0){return he((e,i)=>{i.add(t.schedule(()=>e.subscribe(i),n))})}function Xy(t,n){return Ze(t).pipe(Gd(n),$d(n))}function Ky(t,n){return Ze(t).pipe(Gd(n),$d(n))}function Jy(t,n){return new ce(e=>{let i=0;return n.schedule(function(){i===t.length?e.complete():(e.next(t[i++]),e.closed||this.schedule())})})}function e0(t,n){return new ce(e=>{let i;return wn(e,n,()=>{i=t[Vd](),wn(e,n,()=>{let r,o;try{({value:r,done:o}=i.next())}catch(s){e.error(s);return}o?e.complete():e.next(r)},0,!0)}),()=>ue(i?.return)&&i.return()})}function Wd(t,n){if(!t)throw new Error("Iterable cannot be null");return new ce(e=>{wn(e,n,()=>{let i=t[Symbol.asyncIterator]();wn(e,n,()=>{i.next().then(r=>{r.done?e.complete():e.next(r.value)})},0,!0)})})}function t0(t,n){return Wd(Ud(t),n)}function n0(t,n){if(t!=null){if(Ld(t))return Xy(t,n);if(Pd(t))return Jy(t,n);if(Fd(t))return Ky(t,n);if(jd(t))return Wd(t,n);if(Hd(t))return e0(t,n);if(zd(t))return t0(t,n)}throw Bd(t)}function ot(t,n){return n?n0(t,n):Ze(t)}function W(...t){let n=Ti(t);return ot(t,n)}function Ao(t,n){let e=ue(t)?t:()=>t,i=r=>r.error(e());return new ce(n?r=>n.schedule(i,0,r):i)}function lc(t){return!!t&&(t instanceof ce||ue(t.lift)&&ue(t.subscribe))}var ai=Ts(t=>function(){t(this),this.name="EmptyError",this.message="no elements in sequence"});function i0(t,n){let e=typeof n=="object";return new Promise((i,r)=>{let o=!1,s;t.subscribe({next:a=>{s=a,o=!0},error:r,complete:()=>{o?i(s):e?i(n.defaultValue):r(new ai)}})})}function Ro(t,n){let e=typeof n=="object";return new Promise((i,r)=>{let o=new si({next:s=>{i(s),o.unsubscribe()},error:r,complete:()=>{e?i(n.defaultValue):r(new ai)}});t.subscribe(o)})}function r0(t){return t instanceof Date&&!isNaN(t)}function Q(t,n){return he((e,i)=>{let r=0;e.subscribe(_e(i,o=>{i.next(t.call(n,o,r++))}))})}var{isArray:hM}=Array;function pM(t,n){return hM(n)?t(...n):t(n)}function qd(t){return Q(n=>pM(t,n))}var{isArray:gM}=Array,{getPrototypeOf:_M,prototype:vM,keys:bM}=Object;function Yd(t){if(t.length===1){let n=t[0];if(gM(n))return{args:n,keys:null};if(yM(n)){let e=bM(n);return{args:e.map(i=>n[i]),keys:e}}}return{args:t,keys:null}}function yM(t){return t&&typeof t=="object"&&_M(t)===vM}function Zd(t,n){return t.reduce((e,i,r)=>(e[i]=n[r],e),{})}function dc(...t){let n=Ti(t),e=Nd(t),{args:i,keys:r}=Yd(t);if(i.length===0)return ot([],n);let o=new ce(xM(i,n,r?s=>Zd(r,s):On));return e?o.pipe(qd(e)):o}function xM(t,n,e=On){return i=>{o0(n,()=>{let{length:r}=t,o=new Array(r),s=r,a=r;for(let c=0;c<r;c++)o0(n,()=>{let l=ot(t[c],n),u=!1;l.subscribe(_e(i,m=>{o[c]=m,u||(u=!0,a--),a||i.next(e(o.slice()))},()=>{--s||i.complete()}))},i)},i)}}function o0(t,n,e){t?wn(e,t,n):n()}function s0(t,n,e,i,r,o,s,a){let c=[],l=0,u=0,m=!1,g=()=>{m&&!c.length&&!l&&n.complete()},v=F=>l<i?x(F):c.push(F),x=F=>{o&&n.next(F),l++;let j=!1;Ze(e(F,u++)).subscribe(_e(n,Y=>{r?.(Y),o?v(Y):n.next(Y)},()=>{j=!0},void 0,()=>{if(j)try{for(l--;c.length&&l<i;){let Y=c.shift();s?wn(n,s,()=>x(Y)):x(Y)}g()}catch(Y){n.error(Y)}}))};return t.subscribe(_e(n,v,()=>{m=!0,g()})),()=>{a?.()}}function tn(t,n,e=1/0){return ue(n)?tn((i,r)=>Q((o,s)=>n(i,o,r,s))(Ze(t(i,r))),e):(typeof n=="number"&&(e=n),he((i,r)=>s0(i,r,t,e)))}function Fr(t=1/0){return tn(On,t)}function a0(){return Fr(1)}function Lr(...t){return a0()(ot(t,Ti(t)))}function ci(t){return new ce(n=>{Ze(t()).subscribe(n)})}function Oo(...t){let n=Nd(t),{args:e,keys:i}=Yd(t),r=new ce(o=>{let{length:s}=e;if(!s){o.complete();return}let a=new Array(s),c=s,l=s;for(let u=0;u<s;u++){let m=!1;Ze(e[u]).subscribe(_e(o,g=>{m||(m=!0,l--),a[u]=g},()=>c--,void 0,()=>{(!c||!m)&&(l||o.next(i?Zd(i,a):a),o.complete())}))}});return n?r.pipe(qd(n)):r}function c0(t=0,n,e=$y){let i=-1;return n!=null&&(Od(n)?e=n:i=n),new ce(r=>{let o=r0(t)?+t-e.now():t;o<0&&(o=0);let s=0;return e.schedule(function(){r.closed||(r.next(s++),0<=i?this.schedule(void 0,i):r.complete())},o)})}function mn(...t){let n=Ti(t),e=Gy(t,1/0),i=t;return i.length?i.length===1?Ze(i[0]):Fr(e)(ot(i,n)):St}function De(t,n){return he((e,i)=>{let r=0;e.subscribe(_e(i,o=>t.call(n,o,r++)&&i.next(o)))})}function l0(t){return he((n,e)=>{let i=!1,r=null,o=null,s=!1,a=()=>{if(o?.unsubscribe(),o=null,i){i=!1;let l=r;r=null,e.next(l)}s&&e.complete()},c=()=>{o=null,s&&e.complete()};n.subscribe(_e(e,l=>{i=!0,r=l,o||Ze(t(l)).subscribe(o=_e(e,a,c))},()=>{s=!0,(!i||!o||o.closed)&&e.complete()}))})}function Qd(t,n=cc){return l0(()=>c0(t,n))}function li(t){return he((n,e)=>{let i=null,r=!1,o;i=n.subscribe(_e(e,void 0,void 0,s=>{o=Ze(t(s,li(t)(n))),i?(i.unsubscribe(),i=null,o.subscribe(e)):r=!0})),r&&(i.unsubscribe(),i=null,o.subscribe(e))})}function No(t,n){return ue(n)?tn(t,n,1):tn(t,1)}function Po(t,n=cc){return he((e,i)=>{let r=null,o=null,s=null,a=()=>{if(r){r.unsubscribe(),r=null;let l=o;o=null,i.next(l)}};function c(){let l=s+t,u=n.now();if(u<l){r=this.schedule(void 0,l-u),i.add(r);return}a()}e.subscribe(_e(i,l=>{o=l,s=n.now(),r||(r=n.schedule(c,t),i.add(r))},()=>{a(),i.complete()},void 0,()=>{o=r=null}))})}function d0(t){return he((n,e)=>{let i=!1;n.subscribe(_e(e,r=>{i=!0,e.next(r)},()=>{i||e.next(t),e.complete()}))})}function ut(t){return t<=0?()=>St:he((n,e)=>{let i=0;n.subscribe(_e(e,r=>{++i<=t&&(e.next(r),t<=i&&e.complete())}))})}function Xd(t,n=On){return t=t??CM,he((e,i)=>{let r,o=!0;e.subscribe(_e(i,s=>{let a=n(s);(o||!t(r,a))&&(o=!1,r=a,i.next(s))}))})}function CM(t,n){return t===n}function u0(t=wM){return he((n,e)=>{let i=!1;n.subscribe(_e(e,r=>{i=!0,e.next(r)},()=>i?e.complete():e.error(t())))})}function wM(){return new ai}function or(t){return he((n,e)=>{try{n.subscribe(e)}finally{e.add(t)}})}function sr(t,n){let e=arguments.length>=2;return i=>i.pipe(t?De((r,o)=>t(r,o,i)):On,ut(1),e?d0(n):u0(()=>new ai))}function Kd(t){return t<=0?()=>St:he((n,e)=>{let i=[];n.subscribe(_e(e,r=>{i.push(r),t<i.length&&i.shift()},()=>{for(let r of i)e.next(r);e.complete()},void 0,()=>{i=null}))})}function Jd(){return he((t,n)=>{let e,i=!1;t.subscribe(_e(n,r=>{let o=e;e=r,i&&n.next([o,r]),i=!0}))})}function uc(t={}){let{connector:n=()=>new I,resetOnError:e=!0,resetOnComplete:i=!0,resetOnRefCountZero:r=!0}=t;return o=>{let s,a,c,l=0,u=!1,m=!1,g=()=>{a?.unsubscribe(),a=void 0},v=()=>{g(),s=c=void 0,u=m=!1},x=()=>{let F=s;v(),F?.unsubscribe()};return he((F,j)=>{l++,!m&&!u&&g();let Y=c=c??n();j.add(()=>{l--,l===0&&!m&&!u&&(a=yp(x,r))}),Y.subscribe(j),!s&&l>0&&(s=new si({next:be=>Y.next(be),error:be=>{m=!0,g(),a=yp(v,e,be),Y.error(be)},complete:()=>{u=!0,g(),a=yp(v,i),Y.complete()}}),Ze(F).subscribe(s))})(o)}}function yp(t,n,...e){if(n===!0){t();return}if(n===!1)return;let i=new si({next:()=>{i.unsubscribe(),t()}});return Ze(n(...e)).subscribe(i)}function eu(t,n,e){let i,r=!1;return t&&typeof t=="object"?{bufferSize:i=1/0,windowTime:n=1/0,refCount:r=!1,scheduler:e}=t:i=t??1/0,uc({connector:()=>new Mi(i,n,e),resetOnError:!0,resetOnComplete:!1,resetOnRefCountZero:r})}function mc(t){return De((n,e)=>t<=e)}function et(...t){let n=Ti(t);return he((e,i)=>{(n?Lr(t,e,n):Lr(t,e)).subscribe(i)})}function gt(t,n){return he((e,i)=>{let r=null,o=0,s=!1,a=()=>s&&!r&&i.complete();e.subscribe(_e(i,c=>{r?.unsubscribe();let l=0,u=o++;Ze(t(c,u)).subscribe(r=_e(i,m=>i.next(n?n(c,m,u,l++):m),()=>{r=null,a()}))},()=>{s=!0,a()}))})}function me(t){return he((n,e)=>{Ze(t).subscribe(_e(e,()=>e.complete(),oc)),!e.closed&&n.subscribe(e)})}function xp(t,n=!1){return he((e,i)=>{let r=0;e.subscribe(_e(i,o=>{let s=t(o,r++);(s||n)&&i.next(o),!s&&i.complete()}))})}function st(t,n,e){let i=ue(t)||n||e?{next:t,error:n,complete:e}:t;return i?he((r,o)=>{var s;(s=i.subscribe)===null||s===void 0||s.call(i);let a=!0;r.subscribe(_e(o,c=>{var l;(l=i.next)===null||l===void 0||l.call(i,c),o.next(c)},()=>{var c;a=!1,(c=i.complete)===null||c===void 0||c.call(i),o.complete()},c=>{var l;a=!1,(l=i.error)===null||l===void 0||l.call(i,c),o.error(c)},()=>{var c,l;a&&((c=i.unsubscribe)===null||c===void 0||c.call(i)),(l=i.finalize)===null||l===void 0||l.call(i)}))}):On}var Cp;function fc(){return Cp}function Ai(t){let n=Cp;return Cp=t,n}function Dn(t,n){let e=fc();if(!e)throw new Error("Current injector is not set.");if(!t.\u0275prov)throw new Error("Token is not an injectable");return e.retrieve(t,n)}var m0=Symbol("NotFound");function Ps(t){return t===m0||t?.name==="\u0275NotFound"}function wp(t,n,e){let i=Object.create(DM);i.source=t,i.computation=n,e!=null&&(i.equal=e);let o=()=>{if(Eo(i),Or(i),i.value===ki)throw i.error;return i.value};return o[Et]=i,tc(i),o}function f0(t,n){Eo(t),So(t,n),Is(t)}function h0(t,n){if(Eo(t),t.value===ki)throw t.error;Dd(t,n),Is(t)}var DM=Z(b({},Rr),{value:wo,dirty:!0,error:null,equal:nc,kind:"linkedSignal",producerMustRecompute(t){return t.value===wo||t.value===Do},producerRecomputeValue(t){if(t.value===Do)throw new Error("");let n=t.value;t.value=Do;let e=rr(t),i,r=!1;try{let o=t.source(),s=n!==wo&&n!==ki,a=s?{source:t.sourceValue,value:n}:void 0;i=t.computation(o,a),t.sourceValue=o,te(null),r=s&&i!==ki&&t.equal(n,i)}catch(o){i=ki,t.error=o}finally{Nr(t,e)}if(r){t.value=n;return}t.value=i,t.version++}});function p0(t){let n=te(null);try{return t()}finally{te(n)}}var au="https://angular.dev/best-practices/security#preventing-cross-site-scripting-xss",R=class extends Error{code;constructor(n,e){super(Oi(n,e)),this.code=n}};function EM(t){return`NG0${Math.abs(t)}`}function Oi(t,n){return`${EM(t)}${n?": "+n:""}`}var Gn=globalThis;function je(t){for(let n in t)if(t[n]===je)return n;throw Error("")}function y0(t,n){for(let e in n)n.hasOwnProperty(e)&&!t.hasOwnProperty(e)&&(t[e]=n[e])}function yc(t){if(typeof t=="string")return t;if(Array.isArray(t))return`[${t.map(yc).join(", ")}]`;if(t==null)return""+t;let n=t.overriddenName||t.name;if(n)return`${n}`;let e=t.toString();if(e==null)return""+e;let i=e.indexOf(`
`);return i>=0?e.slice(0,i):e}function cu(t,n){return t?n?`${t} ${n}`:t:n||""}var SM=je({__forward_ref__:je});function It(t){return t.__forward_ref__=It,t}function Qt(t){return Fp(t)?t():t}function Fp(t){return typeof t=="function"&&t.hasOwnProperty(SM)&&t.__forward_ref__===It}function C(t){return{token:t.token,providedIn:t.providedIn||null,factory:t.factory,value:void 0}}function oe(t){return{providers:t.providers||[],imports:t.imports||[]}}function xc(t){return IM(t,lu)}function Lp(t){return xc(t)!==null}function IM(t,n){return t.hasOwnProperty(n)&&t[n]||null}function kM(t){let n=t?.[lu]??null;return n||null}function Ep(t){return t&&t.hasOwnProperty(nu)?t[nu]:null}var lu=je({\u0275prov:je}),nu=je({\u0275inj:je}),y=class{_desc;ngMetadataName="InjectionToken";\u0275prov;constructor(n,e){this._desc=n,this.\u0275prov=void 0,typeof e=="number"?this.__NG_ELEMENT_ID__=e:e!==void 0&&(this.\u0275prov=C({token:this,providedIn:e.providedIn||"root",factory:e.factory}))}get multi(){return this}toString(){return`InjectionToken ${this._desc}`}};function jp(t){return t&&!!t.\u0275providers}var Cc=je({\u0275cmp:je}),wc=je({\u0275dir:je}),Bp=je({\u0275pipe:je}),Vp=je({\u0275mod:je}),pc=je({\u0275fac:je}),Vo=je({__NG_ELEMENT_ID__:je}),g0=je({__NG_ENV_ID__:je});function Hp(t){return du(t,"@NgModule"),t[Vp]||null}function cr(t){return du(t,"@Component"),t[Cc]||null}function Up(t){return du(t,"@Directive"),t[wc]||null}function x0(t){return du(t,"@Pipe"),t[Bp]||null}function du(t,n){if(t==null)throw new R(-919,!1)}function Ho(t){return typeof t=="string"?t:t==null?"":String(t)}var C0=je({ngErrorCode:je}),MM=je({ngErrorMessage:je}),TM=je({ngTokenPath:je});function zp(t,n){return w0("",-200,n)}function uu(t,n){throw new R(-201,!1)}function w0(t,n,e){let i=new R(n,t);return i[C0]=n,i[MM]=t,e&&(i[TM]=e),i}function AM(t){return t[C0]}var Sp;function D0(){return Sp}function fn(t){let n=Sp;return Sp=t,n}function $p(t,n,e){let i=xc(t);if(i&&i.providedIn=="root")return i.value===void 0?i.value=i.factory():i.value;if(e&8)return null;if(n!==void 0)return n;uu(t,"")}var RM={},Fo=RM,OM="__NG_DI_FLAG__",Ip=class{injector;constructor(n){this.injector=n}retrieve(n,e){let i=Lo(e)||0;try{return this.injector.get(n,i&8?null:Fo,i)}catch(r){if(Ps(r))return r;throw r}}};function NM(t,n=0){let e=fc();if(e===void 0)throw new R(-203,!1);if(e===null)return $p(t,void 0,n);{let i=PM(n),r=e.retrieve(t,i);if(Ps(r)){if(i.optional)return null;throw r}return r}}function J(t,n=0){return(D0()||NM)(Qt(t),n)}function d(t,n){return J(t,Lo(n))}function Lo(t){return typeof t>"u"||typeof t=="number"?t:0|(t.optional&&8)|(t.host&&1)|(t.self&&2)|(t.skipSelf&&4)}function PM(t){return{optional:!!(t&8),host:!!(t&1),self:!!(t&2),skipSelf:!!(t&4)}}function kp(t){let n=[];for(let e=0;e<t.length;e++){let i=Qt(t[e]);if(Array.isArray(i)){if(i.length===0)throw new R(900,!1);let r,o=0;for(let s=0;s<i.length;s++){let a=i[s],c=FM(a);typeof c=="number"?c===-1?r=a.token:o|=c:r=a}n.push(J(r,o))}else n.push(J(i))}return n}function FM(t){return t[OM]}function jr(t,n){let e=t.hasOwnProperty(pc);return e?t[pc]:null}function E0(t,n,e){if(t.length!==n.length)return!1;for(let i=0;i<t.length;i++){let r=t[i],o=n[i];if(e&&(r=e(r),o=e(o)),o!==r)return!1}return!0}function S0(t){return t.flat(Number.POSITIVE_INFINITY)}function mu(t,n){t.forEach(e=>Array.isArray(e)?mu(e,n):n(e))}function Gp(t,n,e){n>=t.length?t.push(e):t.splice(n,0,e)}function Dc(t,n){return n>=t.length-1?t.pop():t.splice(n,1)[0]}function I0(t,n){let e=[];for(let i=0;i<t;i++)e.push(n);return e}function k0(t,n,e,i){let r=t.length;if(r==n)t.push(e,i);else if(r===1)t.push(i,t[0]),t[0]=e;else{for(r--,t.push(t[r-1],t[r]);r>n;){let o=r-2;t[r]=t[o],r--}t[n]=e,t[n+1]=i}}function fu(t,n,e){let i=Ls(t,n);return i>=0?t[i|1]=e:(i=~i,k0(t,i,n,e)),i}function hu(t,n){let e=Ls(t,n);if(e>=0)return t[e|1]}function Ls(t,n){return LM(t,n,1)}function LM(t,n,e){let i=0,r=t.length>>e;for(;r!==i;){let o=i+(r-i>>1),s=t[o<<e];if(n===s)return o<<e;s>n?r=o:i=o+1}return~(r<<e)}var Hr={},nn=[],Ur=new y(""),Wp=new y("",-1),qp=new y(""),gc=class{get(n,e=Fo){if(e===Fo){let r=w0("",-201);throw r.name="\u0275NotFound",r}return e}};function Ni(t){return{\u0275providers:t}}function M0(t){return Ni([{provide:Ur,multi:!0,useValue:t}])}function T0(...t){return{\u0275providers:Yp(!0,t),\u0275fromNgModule:!0}}function Yp(t,...n){let e=[],i=new Set,r,o=s=>{e.push(s)};return mu(n,s=>{let a=s;iu(a,o,[],i)&&(r||=[],r.push(a))}),r!==void 0&&A0(r,o),e}function A0(t,n){for(let e=0;e<t.length;e++){let{ngModule:i,providers:r}=t[e];Zp(r,o=>{n(o,i)})}}function iu(t,n,e,i){if(t=Qt(t),!t)return!1;let r=null,o=Ep(t),s=!o&&cr(t);if(!o&&!s){let c=t.ngModule;if(o=Ep(c),o)r=c;else return!1}else{if(s&&!s.standalone)return!1;r=t}let a=i.has(r);if(s){if(a)return!1;if(i.add(r),s.dependencies){let c=typeof s.dependencies=="function"?s.dependencies():s.dependencies;for(let l of c)iu(l,n,e,i)}}else if(o){if(o.imports!=null&&!a){i.add(r);let l;mu(o.imports,u=>{iu(u,n,e,i)&&(l||=[],l.push(u))}),l!==void 0&&A0(l,n)}if(!a){let l=jr(r)||(()=>new r);n({provide:r,useFactory:l,deps:nn},r),n({provide:qp,useValue:r,multi:!0},r),n({provide:Ur,useValue:()=>J(r),multi:!0},r)}let c=o.providers;if(c!=null&&!a){let l=t;Zp(c,u=>{n(u,l)})}}else return!1;return r!==t&&t.providers!==void 0}function Zp(t,n){for(let e of t)jp(e)&&(e=e.\u0275providers),Array.isArray(e)?Zp(e,n):n(e)}var jM=je({provide:String,useValue:je});function R0(t){return t!==null&&typeof t=="object"&&jM in t}function BM(t){return!!(t&&t.useExisting)}function VM(t){return!!(t&&t.useFactory)}function jo(t){return typeof t=="function"}function O0(t){return!!t.useClass}var Ec=new y(""),tu={},_0={},Dp;function js(){return Dp===void 0&&(Dp=new gc),Dp}var He=class{},Bo=class extends He{parent;source;scopes;records=new Map;_ngOnDestroyHooks=new Set;_onDestroyHooks=[];get destroyed(){return this._destroyed}_destroyed=!1;injectorDefTypes;constructor(n,e,i,r){super(),this.parent=e,this.source=i,this.scopes=r,Tp(n,s=>this.processProvider(s)),this.records.set(Wp,Fs(void 0,this)),r.has("environment")&&this.records.set(He,Fs(void 0,this));let o=this.records.get(Ec);o!=null&&typeof o.value=="string"&&this.scopes.add(o.value),this.injectorDefTypes=new Set(this.get(qp,nn,{self:!0}))}retrieve(n,e){let i=Lo(e)||0;try{return this.get(n,Fo,i)}catch(r){if(Ps(r))return r;throw r}}destroy(){hc(this),this._destroyed=!0;let n=te(null);try{for(let i of this._ngOnDestroyHooks)i.ngOnDestroy();let e=this._onDestroyHooks;this._onDestroyHooks=[];for(let i of e)i()}finally{this.records.clear(),this._ngOnDestroyHooks.clear(),this.injectorDefTypes.clear(),te(n)}}onDestroy(n){return hc(this),this._onDestroyHooks.push(n),()=>this.removeOnDestroy(n)}runInContext(n){hc(this);let e=Ai(this),i=fn(void 0),r;try{return n()}finally{Ai(e),fn(i)}}get(n,e=Fo,i){if(hc(this),n.hasOwnProperty(g0))return n[g0](this);let r=Lo(i),o,s=Ai(this),a=fn(void 0);try{if(!(r&4)){let l=this.records.get(n);if(l===void 0){let u=GM(n)&&xc(n);u&&this.injectableDefInScope(u)?l=Fs(Mp(n),tu):l=null,this.records.set(n,l)}if(l!=null)return this.hydrate(n,l,r)}let c=r&2?js():this.parent;return e=r&8&&e===Fo?null:e,c.get(n,e)}catch(c){let l=AM(c);throw l===-200||l===-201?new R(l,null):c}finally{fn(a),Ai(s)}}resolveInjectorInitializers(){let n=te(null),e=Ai(this),i=fn(void 0),r;try{let o=this.get(Ur,nn,{self:!0});for(let s of o)s()}finally{Ai(e),fn(i),te(n)}}toString(){return"R3Injector[...]"}processProvider(n){n=Qt(n);let e=jo(n)?n:Qt(n&&n.provide),i=UM(n);if(!jo(n)&&n.multi===!0){let r=this.records.get(e);r||(r=Fs(void 0,tu,!0),r.factory=()=>kp(r.multi),this.records.set(e,r)),e=n,r.multi.push(n)}this.records.set(e,i)}hydrate(n,e,i){let r=te(null);try{if(e.value===_0)throw zp("");return e.value===tu&&(e.value=_0,e.value=e.factory(void 0,i)),typeof e.value=="object"&&e.value&&$M(e.value)&&this._ngOnDestroyHooks.add(e.value),e.value}finally{te(r)}}injectableDefInScope(n){if(!n.providedIn)return!1;let e=Qt(n.providedIn);return typeof e=="string"?e==="any"||this.scopes.has(e):this.injectorDefTypes.has(e)}removeOnDestroy(n){let e=this._onDestroyHooks.indexOf(n);e!==-1&&this._onDestroyHooks.splice(e,1)}};function Mp(t){let n=xc(t),e=n!==null?n.factory:jr(t);if(e!==null)return e;if(t instanceof y)throw new R(-204,!1);if(t instanceof Function)return HM(t);throw new R(-204,!1)}function HM(t){if(t.length>0)throw new R(-204,!1);let e=kM(t);return e!==null?()=>e.factory(t):()=>new t}function UM(t){if(R0(t))return Fs(void 0,t.useValue);{let n=Qp(t);return Fs(n,tu)}}function Qp(t,n,e){let i;if(jo(t)){let r=Qt(t);return jr(r)||Mp(r)}else if(R0(t))i=()=>Qt(t.useValue);else if(VM(t))i=()=>t.useFactory(...kp(t.deps||[]));else if(BM(t))i=(r,o)=>J(Qt(t.useExisting),o!==void 0&&o&8?8:void 0);else{let r=Qt(t&&(t.useClass||t.provide));if(zM(t))i=()=>new r(...kp(t.deps));else return jr(r)||Mp(r)}return i}function hc(t){if(t.destroyed)throw new R(-205,!1)}function Fs(t,n,e=!1){return{factory:t,value:n,multi:e?[]:void 0}}function zM(t){return!!t.deps}function $M(t){return t!==null&&typeof t=="object"&&typeof t.ngOnDestroy=="function"}function GM(t){return typeof t=="function"||typeof t=="object"&&t.ngMetadataName==="InjectionToken"}function Tp(t,n){for(let e of t)Array.isArray(e)?Tp(e,n):e&&jp(e)?Tp(e.\u0275providers,n):n(e)}function $t(t,n){let e;t instanceof Bo?(hc(t),e=t):e=new Ip(t);let i,r=Ai(e),o=fn(void 0);try{return n()}finally{Ai(r),fn(o)}}function N0(){return D0()!==void 0||fc()!=null}var di=0,ie=1,se=2,Lt=3,Wn=4,hn=5,Uo=6,Bs=7,kt=8,lr=9,ui=10,Ue=11,Vs=12,Xp=13,zo=14,pn=15,zr=16,$o=17,Pi=18,dr=19,Kp=20,ar=21,pu=22,Br=23,Nn=24,Go=25,$r=26,at=27,P0=1,Jp=6,Gr=7,Sc=8,Wo=9,_t=10;function ur(t){return Array.isArray(t)&&typeof t[P0]=="object"}function mi(t){return Array.isArray(t)&&t[P0]===!0}function eg(t){return(t.flags&4)!==0}function Fi(t){return t.componentOffset>-1}function Hs(t){return(t.flags&1)===1}function Li(t){return!!t.template}function Us(t){return(t[se]&512)!==0}function qo(t){return(t[se]&256)===256}var tg="svg",F0="math";function qn(t){for(;Array.isArray(t);)t=t[di];return t}function ng(t,n){return qn(n[t])}function Yn(t,n){return qn(n[t.index])}function gu(t,n){return t.data[n]}function _u(t,n){return t[n]}function ig(t,n,e,i){e>=t.data.length&&(t.data[e]=null,t.blueprint[e]=null),n[e]=i}function Zn(t,n){let e=n[t];return ur(e)?e:e[di]}function L0(t){return(t[se]&4)===4}function vu(t){return(t[se]&128)===128}function j0(t){return mi(t[Lt])}function Pn(t,n){return n==null?null:t[n]}function rg(t){t[$o]=0}function og(t){t[se]&1024||(t[se]|=1024,vu(t)&&Yo(t))}function B0(t,n){for(;t>0;)n=n[zo],t--;return n}function Ic(t){return!!(t[se]&9216||t[Nn]?.dirty)}function bu(t){t[ui].changeDetectionScheduler?.notify(8),t[se]&64&&(t[se]|=1024),Ic(t)&&Yo(t)}function Yo(t){t[ui].changeDetectionScheduler?.notify(0);let n=Vr(t);for(;n!==null&&!(n[se]&8192||(n[se]|=8192,!vu(n)));)n=Vr(n)}function sg(t,n){if(qo(t))throw new R(911,!1);t[ar]===null&&(t[ar]=[]),t[ar].push(n)}function V0(t,n){if(t[ar]===null)return;let e=t[ar].indexOf(n);e!==-1&&t[ar].splice(e,1)}function Vr(t){let n=t[Lt];return mi(n)?n[Lt]:n}function ag(t){return t[Bs]??=[]}function cg(t){return t.cleanup??=[]}function H0(t,n,e,i){let r=ag(n);r.push(e),t.firstCreatePass&&cg(t).push(i,r.length-1)}var pe={lFrame:J0(null),bindingsEnabled:!0,skipHydrationRootTNode:null};var Ap=!1;function U0(){return pe.lFrame.elementDepthCount}function z0(){pe.lFrame.elementDepthCount++}function lg(){pe.lFrame.elementDepthCount--}function yu(){return pe.bindingsEnabled}function dg(){return pe.skipHydrationRootTNode!==null}function ug(t){return pe.skipHydrationRootTNode===t}function mg(){pe.skipHydrationRootTNode=null}function ne(){return pe.lFrame.lView}function Qe(){return pe.lFrame.tView}function Te(t){return pe.lFrame.contextLView=t,t[kt]}function Ae(t){return pe.lFrame.contextLView=null,t}function jt(){let t=fg();for(;t!==null&&t.type===64;)t=t.parent;return t}function fg(){return pe.lFrame.currentTNode}function $0(){let t=pe.lFrame,n=t.currentTNode;return t.isParent?n:n.parent}function zs(t,n){let e=pe.lFrame;e.currentTNode=t,e.isParent=n}function hg(){return pe.lFrame.isParent}function pg(){pe.lFrame.isParent=!1}function G0(){return pe.lFrame.contextLView}function gg(){return Ap}function _c(t){let n=Ap;return Ap=t,n}function kc(){let t=pe.lFrame,n=t.bindingRootIndex;return n===-1&&(n=t.bindingRootIndex=t.tView.bindingStartIndex),n}function W0(){return pe.lFrame.bindingIndex}function q0(t){return pe.lFrame.bindingIndex=t}function ji(){return pe.lFrame.bindingIndex++}function xu(t){let n=pe.lFrame,e=n.bindingIndex;return n.bindingIndex=n.bindingIndex+t,e}function Y0(){return pe.lFrame.inI18n}function Z0(t,n){let e=pe.lFrame;e.bindingIndex=e.bindingRootIndex=t,Cu(n)}function Q0(){return pe.lFrame.currentDirectiveIndex}function Cu(t){pe.lFrame.currentDirectiveIndex=t}function X0(t){let n=pe.lFrame.currentDirectiveIndex;return n===-1?null:t[n]}function wu(){return pe.lFrame.currentQueryIndex}function Mc(t){pe.lFrame.currentQueryIndex=t}function WM(t){let n=t[ie];return n.type===2?n.declTNode:n.type===1?t[hn]:null}function _g(t,n,e){if(e&4){let r=n,o=t;for(;r=r.parent,r===null&&!(e&1);)if(r=WM(o),r===null||(o=o[zo],r.type&10))break;if(r===null)return!1;n=r,t=o}let i=pe.lFrame=K0();return i.currentTNode=n,i.lView=t,!0}function Du(t){let n=K0(),e=t[ie];pe.lFrame=n,n.currentTNode=e.firstChild,n.lView=t,n.tView=e,n.contextLView=t,n.bindingIndex=e.bindingStartIndex,n.inI18n=!1}function K0(){let t=pe.lFrame,n=t===null?null:t.child;return n===null?J0(t):n}function J0(t){let n={currentTNode:null,isParent:!0,lView:null,tView:null,selectedIndex:-1,contextLView:null,elementDepthCount:0,currentNamespace:null,currentDirectiveIndex:-1,bindingRootIndex:-1,bindingIndex:-1,currentQueryIndex:0,parent:t,child:null,inI18n:!1};return t!==null&&(t.child=n),n}function ex(){let t=pe.lFrame;return pe.lFrame=t.parent,t.currentTNode=null,t.lView=null,t}var vg=ex;function Eu(){let t=ex();t.isParent=!0,t.tView=null,t.selectedIndex=-1,t.contextLView=null,t.elementDepthCount=0,t.currentDirectiveIndex=-1,t.currentNamespace=null,t.bindingRootIndex=-1,t.bindingIndex=-1,t.currentQueryIndex=0}function tx(t){return(pe.lFrame.contextLView=B0(t,pe.lFrame.contextLView))[kt]}function Bi(){return pe.lFrame.selectedIndex}function Wr(t){pe.lFrame.selectedIndex=t}function $s(){let t=pe.lFrame;return gu(t.tView,t.selectedIndex)}function Gt(){pe.lFrame.currentNamespace=tg}function Vi(){qM()}function qM(){pe.lFrame.currentNamespace=null}function bg(){return pe.lFrame.currentNamespace}var nx=!0;function Su(){return nx}function Tc(t){nx=t}function Rp(t,n=null,e=null,i){let r=yg(t,n,e,i);return r.resolveInjectorInitializers(),r}function yg(t,n=null,e=null,i,r=new Set){let o=[e||nn,T0(t)],s;return new Bo(o,n||js(),s||null,r)}var X=class t{static THROW_IF_NOT_FOUND=Fo;static NULL=new gc;static create(n,e){if(Array.isArray(n))return Rp({name:""},e,n,"");{let i=n.name??"";return Rp({name:i},n.parent,n.providers,i)}}static \u0275prov=C({token:t,providedIn:"any",factory:()=>J(Wp)});static __NG_ELEMENT_ID__=-1},K=new y(""),on=(()=>{class t{static __NG_ELEMENT_ID__=YM;static __NG_ENV_ID__=e=>e}return t})(),ru=class extends on{_lView;constructor(n){super(),this._lView=n}get destroyed(){return qo(this._lView)}onDestroy(n){let e=this._lView;return sg(e,n),()=>V0(e,n)}};function YM(){return new ru(ne())}var ix=!1,rx=new y(""),mr=(()=>{class t{taskId=0;pendingTasks=new Set;destroyed=!1;pendingTask=new Ft(!1);debugTaskTracker=d(rx,{optional:!0});get hasPendingTasks(){return this.destroyed?!1:this.pendingTask.value}get hasPendingTasksObservable(){return this.destroyed?new ce(e=>{e.next(!1),e.complete()}):this.pendingTask}add(){!this.hasPendingTasks&&!this.destroyed&&this.pendingTask.next(!0);let e=this.taskId++;return this.pendingTasks.add(e),this.debugTaskTracker?.add(e),e}has(e){return this.pendingTasks.has(e)}remove(e){this.pendingTasks.delete(e),this.debugTaskTracker?.remove(e),this.pendingTasks.size===0&&this.hasPendingTasks&&this.pendingTask.next(!1)}ngOnDestroy(){this.pendingTasks.clear(),this.hasPendingTasks&&this.pendingTask.next(!1),this.destroyed=!0,this.pendingTask.unsubscribe()}static \u0275prov=C({token:t,providedIn:"root",factory:()=>new t})}return t})(),Op=class extends I{__isAsync;destroyRef=void 0;pendingTasks=void 0;constructor(n=!1){super(),this.__isAsync=n,N0()&&(this.destroyRef=d(on,{optional:!0})??void 0,this.pendingTasks=d(mr,{optional:!0})??void 0)}emit(n){let e=te(null);try{super.next(n)}finally{te(e)}}subscribe(n,e,i){let r=n,o=e||(()=>null),s=i;if(n&&typeof n=="object"){let c=n;r=c.next?.bind(c),o=c.error?.bind(c),s=c.complete?.bind(c)}this.__isAsync&&(o=this.wrapInTimeout(o),r&&(r=this.wrapInTimeout(r)),s&&(s=this.wrapInTimeout(s)));let a=super.subscribe({next:r,error:o,complete:s});return n instanceof ge&&n.add(a),a}wrapInTimeout(n){return e=>{let i=this.pendingTasks?.add();setTimeout(()=>{try{n(e)}finally{i!==void 0&&this.pendingTasks?.remove(i)}})}}},U=Op;function ou(...t){}function xg(t){let n,e;function i(){t=ou;try{e!==void 0&&typeof cancelAnimationFrame=="function"&&cancelAnimationFrame(e),n!==void 0&&clearTimeout(n)}catch{}}return n=setTimeout(()=>{t(),i()}),typeof requestAnimationFrame=="function"&&(e=requestAnimationFrame(()=>{t(),i()})),()=>i()}function ox(t){return queueMicrotask(()=>t()),()=>{t=ou}}var Cg="isAngularZone",vc=Cg+"_ID",ZM=0,H=class t{hasPendingMacrotasks=!1;hasPendingMicrotasks=!1;isStable=!0;onUnstable=new U(!1);onMicrotaskEmpty=new U(!1);onStable=new U(!1);onError=new U(!1);constructor(n){let{enableLongStackTrace:e=!1,shouldCoalesceEventChangeDetection:i=!1,shouldCoalesceRunChangeDetection:r=!1,scheduleInRootZone:o=ix}=n;if(typeof Zone>"u")throw new R(908,!1);Zone.assertZonePatched();let s=this;s._nesting=0,s._outer=s._inner=Zone.current,Zone.TaskTrackingZoneSpec&&(s._inner=s._inner.fork(new Zone.TaskTrackingZoneSpec)),e&&Zone.longStackTraceZoneSpec&&(s._inner=s._inner.fork(Zone.longStackTraceZoneSpec)),s.shouldCoalesceEventChangeDetection=!r&&i,s.shouldCoalesceRunChangeDetection=r,s.callbackScheduled=!1,s.scheduleInRootZone=o,KM(s)}static isInAngularZone(){return typeof Zone<"u"&&Zone.current.get(Cg)===!0}static assertInAngularZone(){if(!t.isInAngularZone())throw new R(909,!1)}static assertNotInAngularZone(){if(t.isInAngularZone())throw new R(909,!1)}run(n,e,i){return this._inner.run(n,e,i)}runTask(n,e,i,r){let o=this._inner,s=o.scheduleEventTask("NgZoneEvent: "+r,n,QM,ou,ou);try{return o.runTask(s,e,i)}finally{o.cancelTask(s)}}runGuarded(n,e,i){return this._inner.runGuarded(n,e,i)}runOutsideAngular(n){return this._outer.run(n)}},QM={};function wg(t){if(t._nesting==0&&!t.hasPendingMicrotasks&&!t.isStable)try{t._nesting++,t.onMicrotaskEmpty.emit(null)}finally{if(t._nesting--,!t.hasPendingMicrotasks)try{t.runOutsideAngular(()=>t.onStable.emit(null))}finally{t.isStable=!0}}}function XM(t){if(t.isCheckStableRunning||t.callbackScheduled)return;t.callbackScheduled=!0;function n(){xg(()=>{t.callbackScheduled=!1,Np(t),t.isCheckStableRunning=!0,wg(t),t.isCheckStableRunning=!1})}t.scheduleInRootZone?Zone.root.run(()=>{n()}):t._outer.run(()=>{n()}),Np(t)}function KM(t){let n=()=>{XM(t)},e=ZM++;t._inner=t._inner.fork({name:"angular",properties:{[Cg]:!0,[vc]:e,[vc+e]:!0},onInvokeTask:(i,r,o,s,a,c)=>{if(JM(c))return i.invokeTask(o,s,a,c);try{return v0(t),i.invokeTask(o,s,a,c)}finally{(t.shouldCoalesceEventChangeDetection&&s.type==="eventTask"||t.shouldCoalesceRunChangeDetection)&&n(),b0(t)}},onInvoke:(i,r,o,s,a,c,l)=>{try{return v0(t),i.invoke(o,s,a,c,l)}finally{t.shouldCoalesceRunChangeDetection&&!t.callbackScheduled&&!eT(c)&&n(),b0(t)}},onHasTask:(i,r,o,s)=>{i.hasTask(o,s),r===o&&(s.change=="microTask"?(t._hasPendingMicrotasks=s.microTask,Np(t),wg(t)):s.change=="macroTask"&&(t.hasPendingMacrotasks=s.macroTask))},onHandleError:(i,r,o,s)=>(i.handleError(o,s),t.runOutsideAngular(()=>t.onError.emit(s)),!1)})}function Np(t){t._hasPendingMicrotasks||(t.shouldCoalesceEventChangeDetection||t.shouldCoalesceRunChangeDetection)&&t.callbackScheduled===!0?t.hasPendingMicrotasks=!0:t.hasPendingMicrotasks=!1}function v0(t){t._nesting++,t.isStable&&(t.isStable=!1,t.onUnstable.emit(null))}function b0(t){t._nesting--,wg(t)}var bc=class{hasPendingMicrotasks=!1;hasPendingMacrotasks=!1;isStable=!0;onUnstable=new U;onMicrotaskEmpty=new U;onStable=new U;onError=new U;run(n,e,i){return n.apply(e,i)}runGuarded(n,e,i){return n.apply(e,i)}runOutsideAngular(n){return n()}runTask(n,e,i,r){return n.apply(e,i)}};function JM(t){return sx(t,"__ignore_ng_zone__")}function eT(t){return sx(t,"__scheduler_tick__")}function sx(t,n){return!Array.isArray(t)||t.length!==1?!1:t[0]?.data?.[n]===!0}var rn=class{_console=console;handleError(n){this._console.error("ERROR",n)}},Fn=new y("",{factory:()=>{let t=d(H),n=d(He),e;return i=>{t.runOutsideAngular(()=>{n.destroyed&&!e?setTimeout(()=>{throw i}):(e??=n.get(rn),e.handleError(i))})}}}),ax={provide:Ur,useValue:()=>{let t=d(rn,{optional:!0})},multi:!0},tT=new y("",{factory:()=>{let t=d(K).defaultView;if(!t)return;let n=d(Fn),e=o=>{n(o.reason),o.preventDefault()},i=o=>{o.error?n(o.error):n(new Error(o.message,{cause:o})),o.preventDefault()},r=()=>{t.addEventListener("unhandledrejection",e),t.addEventListener("error",i)};typeof Zone<"u"?Zone.root.run(r):r(),d(on).onDestroy(()=>{t.removeEventListener("error",i),t.removeEventListener("unhandledrejection",e)})}});function Dg(){return Ni([M0(()=>{d(tT)})])}function M(t,n){let[e,i,r]=cp(t,n?.equal),o=e,s=o[Et];return o.set=i,o.update=r,o.asReadonly=Iu.bind(o),o}function Iu(){let t=this[Et];if(t.readonlyFn===void 0){let n=()=>this();n[Et]=t,t.readonlyFn=n}return t.readonlyFn}var Gs=(()=>{class t{view;node;constructor(e,i){this.view=e,this.node=i}static __NG_ELEMENT_ID__=nT}return t})();function nT(){return new Gs(ne(),jt())}var Ri=class{},Ac=new y("",{factory:()=>!0});var Eg=new y(""),Rc=(()=>{class t{internalPendingTasks=d(mr);scheduler=d(Ri);errorHandler=d(Fn);add(){let e=this.internalPendingTasks.add();return()=>{this.internalPendingTasks.has(e)&&(this.scheduler.notify(11),this.internalPendingTasks.remove(e))}}run(e){let i=this.add();e().catch(this.errorHandler).finally(i)}static \u0275prov=C({token:t,providedIn:"root",factory:()=>new t})}return t})(),ku=(()=>{class t{static \u0275prov=C({token:t,providedIn:"root",factory:()=>new Pp})}return t})(),Pp=class{dirtyEffectCount=0;queues=new Map;add(n){this.enqueue(n),this.schedule(n)}schedule(n){n.dirty&&this.dirtyEffectCount++}remove(n){let e=n.zone,i=this.queues.get(e);i.has(n)&&(i.delete(n),n.dirty&&this.dirtyEffectCount--)}enqueue(n){let e=n.zone;this.queues.has(e)||this.queues.set(e,new Set);let i=this.queues.get(e);i.has(n)||i.add(n)}flush(){for(;this.dirtyEffectCount>0;){let n=!1;for(let[e,i]of this.queues)e===null?n||=this.flushQueue(i):n||=e.run(()=>this.flushQueue(i));n||(this.dirtyEffectCount=0)}}flushQueue(n){let e=!1;for(let i of n)i.dirty&&(this.dirtyEffectCount--,e=!0,i.run());return e}},su=class{[Et];constructor(n){this[Et]=n}destroy(){this[Et].destroy()}};function fr(t,n){let e=n?.injector??d(X),i=n?.manualCleanup!==!0?e.get(on):null,r,o=e.get(Gs,null,{optional:!0}),s=e.get(Ri);return o!==null?(r=oT(o.view,s,t),i instanceof ru&&i._lView===o.view&&(i=null)):r=sT(t,e.get(ku),s),r.injector=e,i!==null&&(r.onDestroyFns=[i.onDestroy(()=>r.destroy())]),new su(r)}var cx=Z(b({},lp),{cleanupFns:void 0,zone:null,onDestroyFns:null,run(){let t=_c(!1);try{dp(this)}finally{_c(t)}},cleanup(){if(!this.cleanupFns?.length)return;let t=te(null);try{for(;this.cleanupFns.length;)this.cleanupFns.pop()()}finally{this.cleanupFns=[],te(t)}}}),iT=Z(b({},cx),{consumerMarkedDirty(){this.scheduler.schedule(this),this.notifier.notify(12)},destroy(){if(Pr(this),this.onDestroyFns!==null)for(let t of this.onDestroyFns)t();this.cleanup(),this.scheduler.remove(this)}}),rT=Z(b({},cx),{consumerMarkedDirty(){this.view[se]|=8192,Yo(this.view),this.notifier.notify(13)},destroy(){if(Pr(this),this.onDestroyFns!==null)for(let t of this.onDestroyFns)t();this.cleanup(),this.view[Br]?.delete(this)}});function oT(t,n,e){let i=Object.create(rT);return i.view=t,i.zone=typeof Zone<"u"?Zone.current:null,i.notifier=n,i.fn=lx(i,e),t[Br]??=new Set,t[Br].add(i),i.consumerMarkedDirty(i),i}function sT(t,n,e){let i=Object.create(iT);return i.fn=lx(i,t),i.scheduler=n,i.notifier=e,i.zone=typeof Zone<"u"?Zone.current:null,i.scheduler.add(i),i.notifier.notify(12),i}function lx(t,n){return()=>{n(e=>(t.cleanupFns??=[]).push(e))}}function zc(t){return{toString:t}.toString()}function fT(t){return typeof t=="function"}function $x(t,n,e,i){n!==null?n.applyValueToInputSignal(n,i):t[e]=i}var ju=class{previousValue;currentValue;firstChange;constructor(n,e,i){this.previousValue=n,this.currentValue=e,this.firstChange=i}isFirstChange(){return this.firstChange}},ze=(()=>{let t=()=>Gx;return t.ngInherit=!0,t})();function Gx(t){return t.type.prototype.ngOnChanges&&(t.setInput=pT),hT}function hT(){let t=qx(this),n=t?.current;if(n){let e=t.previous;if(e===Hr)t.previous=n;else for(let i in n)e[i]=n[i];t.current=null,this.ngOnChanges(n)}}function pT(t,n,e,i,r){let o=this.declaredInputs[i],s=qx(t)||gT(t,{previous:Hr,current:null}),a=s.current||(s.current={}),c=s.previous,l=c[o];a[o]=new ju(l&&l.currentValue,e,c===Hr),$x(t,n,r,e)}var Wx="__ngSimpleChanges__";function qx(t){return t[Wx]||null}function gT(t,n){return t[Wx]=n}var dx=[];var Be=function(t,n=null,e){for(let i=0;i<dx.length;i++){let r=dx[i];r(t,n,e)}},Me=(function(t){return t[t.TemplateCreateStart=0]="TemplateCreateStart",t[t.TemplateCreateEnd=1]="TemplateCreateEnd",t[t.TemplateUpdateStart=2]="TemplateUpdateStart",t[t.TemplateUpdateEnd=3]="TemplateUpdateEnd",t[t.LifecycleHookStart=4]="LifecycleHookStart",t[t.LifecycleHookEnd=5]="LifecycleHookEnd",t[t.OutputStart=6]="OutputStart",t[t.OutputEnd=7]="OutputEnd",t[t.BootstrapApplicationStart=8]="BootstrapApplicationStart",t[t.BootstrapApplicationEnd=9]="BootstrapApplicationEnd",t[t.BootstrapComponentStart=10]="BootstrapComponentStart",t[t.BootstrapComponentEnd=11]="BootstrapComponentEnd",t[t.ChangeDetectionStart=12]="ChangeDetectionStart",t[t.ChangeDetectionEnd=13]="ChangeDetectionEnd",t[t.ChangeDetectionSyncStart=14]="ChangeDetectionSyncStart",t[t.ChangeDetectionSyncEnd=15]="ChangeDetectionSyncEnd",t[t.AfterRenderHooksStart=16]="AfterRenderHooksStart",t[t.AfterRenderHooksEnd=17]="AfterRenderHooksEnd",t[t.ComponentStart=18]="ComponentStart",t[t.ComponentEnd=19]="ComponentEnd",t[t.DeferBlockStateStart=20]="DeferBlockStateStart",t[t.DeferBlockStateEnd=21]="DeferBlockStateEnd",t[t.DynamicComponentStart=22]="DynamicComponentStart",t[t.DynamicComponentEnd=23]="DynamicComponentEnd",t[t.HostBindingsUpdateStart=24]="HostBindingsUpdateStart",t[t.HostBindingsUpdateEnd=25]="HostBindingsUpdateEnd",t})(Me||{});function _T(t,n,e){let{ngOnChanges:i,ngOnInit:r,ngDoCheck:o}=n.type.prototype;if(i){let s=Gx(n);(e.preOrderHooks??=[]).push(t,s),(e.preOrderCheckHooks??=[]).push(t,s)}r&&(e.preOrderHooks??=[]).push(0-t,r),o&&((e.preOrderHooks??=[]).push(t,o),(e.preOrderCheckHooks??=[]).push(t,o))}function Yx(t,n){for(let e=n.directiveStart,i=n.directiveEnd;e<i;e++){let o=t.data[e].type.prototype,{ngAfterContentInit:s,ngAfterContentChecked:a,ngAfterViewInit:c,ngAfterViewChecked:l,ngOnDestroy:u}=o;s&&(t.contentHooks??=[]).push(-e,s),a&&((t.contentHooks??=[]).push(e,a),(t.contentCheckHooks??=[]).push(e,a)),c&&(t.viewHooks??=[]).push(-e,c),l&&((t.viewHooks??=[]).push(e,l),(t.viewCheckHooks??=[]).push(e,l)),u!=null&&(t.destroyHooks??=[]).push(e,u)}}function Ou(t,n,e){Zx(t,n,3,e)}function Nu(t,n,e,i){(t[se]&3)===e&&Zx(t,n,e,i)}function Sg(t,n){let e=t[se];(e&3)===n&&(e&=16383,e+=1,t[se]=e)}function Zx(t,n,e,i){let r=i!==void 0?t[$o]&65535:0,o=i??-1,s=n.length-1,a=0;for(let c=r;c<s;c++)if(typeof n[c+1]=="number"){if(a=n[c],i!=null&&a>=i)break}else n[c]<0&&(t[$o]+=65536),(a<o||o==-1)&&(vT(t,e,n,c),t[$o]=(t[$o]&4294901760)+c+2),c++}function ux(t,n){Be(Me.LifecycleHookStart,t,n);let e=te(null);try{n.call(t)}finally{te(e),Be(Me.LifecycleHookEnd,t,n)}}function vT(t,n,e,i){let r=e[i]<0,o=e[i+1],s=r?-e[i]:e[i],a=t[s];r?t[se]>>14<t[$o]>>16&&(t[se]&3)===n&&(t[se]+=16384,ux(a,o)):ux(a,o)}var qs=-1,Qo=class{factory;name;injectImpl;resolving=!1;canSeeViewProviders;multi;componentProviders;index;providerFactory;constructor(n,e,i,r){this.factory=n,this.name=r,this.canSeeViewProviders=e,this.injectImpl=i}};function bT(t){return(t.flags&8)!==0}function yT(t){return(t.flags&16)!==0}function xT(t,n,e){let i=0;for(;i<e.length;){let r=e[i];if(typeof r=="number"){if(r!==0)break;i++;let o=e[i++],s=e[i++],a=e[i++];t.setAttribute(n,s,a,o)}else{let o=r,s=e[++i];CT(o)?t.setProperty(n,o,s):t.setAttribute(n,o,s),i++}}return i}function Qx(t){return t===3||t===4||t===6}function CT(t){return t.charCodeAt(0)===64}function Ys(t,n){if(!(n===null||n.length===0))if(t===null||t.length===0)t=n.slice();else{let e=-1;for(let i=0;i<n.length;i++){let r=n[i];typeof r=="number"?e=r:e===0||(e===-1||e===2?mx(t,e,r,null,n[++i]):mx(t,e,r,null,null))}}return t}function mx(t,n,e,i,r){let o=0,s=t.length;if(n===-1)s=-1;else for(;o<t.length;){let a=t[o++];if(typeof a=="number"){if(a===n){s=-1;break}else if(a>n){s=o-1;break}}}for(;o<t.length;){let a=t[o];if(typeof a=="number")break;if(a===e){r!==null&&(t[o+1]=r);return}o++,r!==null&&o++}s!==-1&&(t.splice(s,0,n),o=s+1),t.splice(o++,0,e),r!==null&&t.splice(o++,0,r)}function Xx(t){return t!==qs}function Bu(t){return t&32767}function wT(t){return t>>16}function Vu(t,n){let e=wT(t),i=n;for(;e>0;)i=i[zo],e--;return i}var Lg=!0;function Hu(t){let n=Lg;return Lg=t,n}var DT=256,Kx=DT-1,Jx=5,ET=0,Hi={};function ST(t,n,e){let i;typeof e=="string"?i=e.charCodeAt(0)||0:e.hasOwnProperty(Vo)&&(i=e[Vo]),i==null&&(i=e[Vo]=ET++);let r=i&Kx,o=1<<r;n.data[t+(r>>Jx)]|=o}function Uu(t,n){let e=eC(t,n);if(e!==-1)return e;let i=n[ie];i.firstCreatePass&&(t.injectorIndex=n.length,Ig(i.data,t),Ig(n,null),Ig(i.blueprint,null));let r=b_(t,n),o=t.injectorIndex;if(Xx(r)){let s=Bu(r),a=Vu(r,n),c=a[ie].data;for(let l=0;l<8;l++)n[o+l]=a[s+l]|c[s+l]}return n[o+8]=r,o}function Ig(t,n){t.push(0,0,0,0,0,0,0,0,n)}function eC(t,n){return t.injectorIndex===-1||t.parent&&t.parent.injectorIndex===t.injectorIndex||n[t.injectorIndex+8]===null?-1:t.injectorIndex}function b_(t,n){if(t.parent&&t.parent.injectorIndex!==-1)return t.parent.injectorIndex;let e=0,i=null,r=n;for(;r!==null;){if(i=oC(r),i===null)return qs;if(e++,r=r[zo],i.injectorIndex!==-1)return i.injectorIndex|e<<16}return qs}function jg(t,n,e){ST(t,n,e)}function IT(t,n){if(n==="class")return t.classes;if(n==="style")return t.styles;let e=t.attrs;if(e){let i=e.length,r=0;for(;r<i;){let o=e[r];if(Qx(o))break;if(o===0)r=r+2;else if(typeof o=="number")for(r++;r<i&&typeof e[r]=="string";)r++;else{if(o===n)return e[r+1];r=r+2}}}return null}function tC(t,n,e){if(e&8||t!==void 0)return t;uu(n,"NodeInjector")}function nC(t,n,e,i){if(e&8&&i===void 0&&(i=null),(e&3)===0){let r=t[lr],o=fn(void 0);try{return r?r.get(n,i,e&8):$p(n,i,e&8)}finally{fn(o)}}return tC(i,n,e)}function iC(t,n,e,i=0,r){if(t!==null){if(n[se]&2048&&!(i&2)){let s=AT(t,n,e,i,Hi);if(s!==Hi)return s}let o=rC(t,n,e,i,Hi);if(o!==Hi)return o}return nC(n,e,i,r)}function rC(t,n,e,i,r){let o=MT(e);if(typeof o=="function"){if(!_g(n,t,i))return i&1?tC(r,e,i):nC(n,e,i,r);try{let s;if(s=o(i),s==null&&!(i&8))uu(e);else return s}finally{vg()}}else if(typeof o=="number"){let s=null,a=eC(t,n),c=qs,l=i&1?n[pn][hn]:null;for((a===-1||i&4)&&(c=a===-1?b_(t,n):n[a+8],c===qs||!hx(i,!1)?a=-1:(s=n[ie],a=Bu(c),n=Vu(c,n)));a!==-1;){let u=n[ie];if(fx(o,a,u.data)){let m=kT(a,n,e,s,i,l);if(m!==Hi)return m}c=n[a+8],c!==qs&&hx(i,n[ie].data[a+8]===l)&&fx(o,a,n)?(s=u,a=Bu(c),n=Vu(c,n)):a=-1}}return r}function kT(t,n,e,i,r,o){let s=n[ie],a=s.data[t+8],c=i==null?Fi(a)&&Lg:i!=s&&(a.type&3)!==0,l=r&1&&o===a,u=Pu(a,s,e,c,l);return u!==null?Lc(n,s,u,a,r):Hi}function Pu(t,n,e,i,r){let o=t.providerIndexes,s=n.data,a=o&1048575,c=t.directiveStart,l=t.directiveEnd,u=o>>20,m=i?a:a+u,g=r?a+u:l;for(let v=m;v<g;v++){let x=s[v];if(v<c&&e===x||v>=c&&x.type===e)return v}if(r){let v=s[c];if(v&&Li(v)&&v.type===e)return c}return null}function Lc(t,n,e,i,r){let o=t[e],s=n.data;if(o instanceof Qo){let a=o;if(a.resolving)throw zp("");let c=Hu(a.canSeeViewProviders);a.resolving=!0;let l=s[e].type||s[e],u,m=a.injectImpl?fn(a.injectImpl):null,g=_g(t,i,0);try{o=t[e]=a.factory(void 0,r,s,t,i),n.firstCreatePass&&e>=i.directiveStart&&_T(e,s[e],n)}finally{m!==null&&fn(m),Hu(c),a.resolving=!1,vg()}}return o}function MT(t){if(typeof t=="string")return t.charCodeAt(0)||0;let n=t.hasOwnProperty(Vo)?t[Vo]:void 0;return typeof n=="number"?n>=0?n&Kx:TT:n}function fx(t,n,e){let i=1<<t;return!!(e[n+(t>>Jx)]&i)}function hx(t,n){return!(t&2)&&!(t&1&&n)}var Zo=class{_tNode;_lView;constructor(n,e){this._tNode=n,this._lView=e}get(n,e,i){return iC(this._tNode,this._lView,n,Lo(i),e)}};function TT(){return new Zo(jt(),ne())}function ct(t){return zc(()=>{let n=t.prototype.constructor,e=n[pc]||Bg(n),i=Object.prototype,r=Object.getPrototypeOf(t.prototype).constructor;for(;r&&r!==i;){let o=r[pc]||Bg(r);if(o&&o!==e)return o;r=Object.getPrototypeOf(r)}return o=>new o})}function Bg(t){return Fp(t)?()=>{let n=Bg(Qt(t));return n&&n()}:jr(t)}function AT(t,n,e,i,r){let o=t,s=n;for(;o!==null&&s!==null&&s[se]&2048&&!Us(s);){let a=rC(o,s,e,i|2,Hi);if(a!==Hi)return a;let c=o.parent;if(!c){let l=s[Kp];if(l){let u=l.get(e,Hi,i&-5);if(u!==Hi)return u}c=oC(s),s=s[zo]}o=c}return r}function oC(t){let n=t[ie],e=n.type;return e===2?n.declTNode:e===1?t[hn]:null}function $c(t){return IT(jt(),t)}function RT(){return Js(jt(),ne())}function Js(t,n){return new L(Yn(t,n))}var L=(()=>{class t{nativeElement;constructor(e){this.nativeElement=e}static __NG_ELEMENT_ID__=RT}return t})();function sC(t){return t instanceof L?t.nativeElement:t}function OT(){return this._results[Symbol.iterator]()}var En=class{_emitDistinctChangesOnly;dirty=!0;_onDirty=void 0;_results=[];_changesDetected=!1;_changes=void 0;length=0;first=void 0;last=void 0;get changes(){return this._changes??=new I}constructor(n=!1){this._emitDistinctChangesOnly=n}get(n){return this._results[n]}map(n){return this._results.map(n)}filter(n){return this._results.filter(n)}find(n){return this._results.find(n)}reduce(n,e){return this._results.reduce(n,e)}forEach(n){this._results.forEach(n)}some(n){return this._results.some(n)}toArray(){return this._results.slice()}toString(){return this._results.toString()}reset(n,e){this.dirty=!1;let i=S0(n);(this._changesDetected=!E0(this._results,i,e))&&(this._results=i,this.length=i.length,this.last=i[this.length-1],this.first=i[0])}notifyOnChanges(){this._changes!==void 0&&(this._changesDetected||!this._emitDistinctChangesOnly)&&this._changes.next(this)}onDirty(n){this._onDirty=n}setDirty(){this.dirty=!0,this._onDirty?.()}destroy(){this._changes!==void 0&&(this._changes.complete(),this._changes.unsubscribe())}[Symbol.iterator]=OT};function aC(t){return(t.flags&128)===128}var y_=(function(t){return t[t.OnPush=0]="OnPush",t[t.Eager=1]="Eager",t[t.Default=1]="Default",t})(y_||{}),cC=new Map,NT=0;function PT(){return NT++}function FT(t){cC.set(t[dr],t)}function Vg(t){cC.delete(t[dr])}var px="__ngContext__";function Zs(t,n){ur(n)?(t[px]=n[dr],FT(n)):t[px]=n}function lC(t){return uC(t[Vs])}function dC(t){return uC(t[Wn])}function uC(t){for(;t!==null&&!mi(t);)t=t[Wn];return t}var LT;function x_(t){LT=t}var Yr=new y("",{factory:()=>jT}),jT="ng";var tm=new y(""),es=new y("",{providedIn:"platform",factory:()=>"unknown"}),Gc=new y(""),ts=new y("",{factory:()=>d(K).body?.querySelector("[ngCspNonce]")?.getAttribute("ngCspNonce")||null});var mC="r";var fC="di";var hC=!1,pC=new y("",{factory:()=>hC});var gx=new WeakMap;function BT(t,n){if(t==null||typeof t!="object")return;let e=gx.get(t);e||(e=new WeakSet,gx.set(t,e)),e.add(n)}var VT=(t,n,e,i)=>{};function HT(t,n,e,i){VT(t,n,e,i)}function nm(t){return(t.flags&32)===32}var UT=()=>null;function gC(t,n,e=!1){return UT(t,n,e)}function _C(t,n){let e=t.contentQueries;if(e!==null){let i=te(null);try{for(let r=0;r<e.length;r+=2){let o=e[r],s=e[r+1];if(s!==-1){let a=t.data[s];Mc(o),a.contentQueries(2,n[s],s)}}}finally{te(i)}}}function Hg(t,n,e){Mc(0);let i=te(null);try{n(t,e)}finally{te(i)}}function C_(t,n,e){if(eg(n)){let i=te(null);try{let r=n.directiveStart,o=n.directiveEnd;for(let s=r;s<o;s++){let a=t.data[s];if(a.contentQueries){let c=e[s];a.contentQueries(1,c,s)}}}finally{te(i)}}}var pi=(function(t){return t[t.Emulated=0]="Emulated",t[t.None=2]="None",t[t.ShadowDom=3]="ShadowDom",t[t.ExperimentalIsolatedShadowDom=4]="ExperimentalIsolatedShadowDom",t})(pi||{});var Mu;function zT(){if(Mu===void 0&&(Mu=null,Gn.trustedTypes))try{Mu=Gn.trustedTypes.createPolicy("angular",{createHTML:t=>t,createScript:t=>t,createScriptURL:t=>t})}catch{}return Mu}function im(t){return zT()?.createHTML(t)||t}var Tu;function $T(){if(Tu===void 0&&(Tu=null,Gn.trustedTypes))try{Tu=Gn.trustedTypes.createPolicy("angular#unsafe-bypass",{createHTML:t=>t,createScript:t=>t,createScriptURL:t=>t})}catch{}return Tu}function _x(t){return $T()?.createScriptURL(t)||t}var hr=class{changingThisBreaksApplicationSecurity;constructor(n){this.changingThisBreaksApplicationSecurity=n}toString(){return`SafeValue must use [property]=binding: ${this.changingThisBreaksApplicationSecurity} (see ${au})`}},Ug=class extends hr{getTypeName(){return"HTML"}},zg=class extends hr{getTypeName(){return"Style"}},$g=class extends hr{getTypeName(){return"Script"}},Gg=class extends hr{getTypeName(){return"URL"}},Wg=class extends hr{getTypeName(){return"ResourceURL"}};function gi(t){return t instanceof hr?t.changingThisBreaksApplicationSecurity:t}function pr(t,n){let e=vC(t);if(e!=null&&e!==n){if(e==="ResourceURL"&&n==="URL")return!0;throw new Error(`Required a safe ${n}, got a ${e} (see ${au})`)}return e===n}function vC(t){return t instanceof hr&&t.getTypeName()||null}function w_(t){return new Ug(t)}function D_(t){return new zg(t)}function E_(t){return new $g(t)}function S_(t){return new Gg(t)}function I_(t){return new Wg(t)}function GT(t){let n=new Yg(t);return WT()?new qg(n):n}var qg=class{inertDocumentHelper;constructor(n){this.inertDocumentHelper=n}getInertBodyElement(n){n="<body><remove></remove>"+n;try{let e=new window.DOMParser().parseFromString(im(n),"text/html").body;return e===null?this.inertDocumentHelper.getInertBodyElement(n):(e.firstChild?.remove(),e)}catch{return null}}},Yg=class{defaultDoc;inertDocument;constructor(n){this.defaultDoc=n,this.inertDocument=this.defaultDoc.implementation.createHTMLDocument("sanitization-inert")}getInertBodyElement(n){let e=this.inertDocument.createElement("template");return e.innerHTML=im(n),e}};function WT(){try{return!!new window.DOMParser().parseFromString(im(""),"text/html")}catch{return!1}}var qT=/^(?!javascript:)(?:[a-z0-9+.-]+:|[^&:\/?#]*(?:[\/?#]|$))/i;function Wc(t){return t=String(t),t.match(qT)?t:"unsafe:"+t}function gr(t){let n={};for(let e of t.split(","))n[e]=!0;return n}function qc(...t){let n={};for(let e of t)for(let i in e)e.hasOwnProperty(i)&&(n[i]=!0);return n}var bC=gr("area,br,col,hr,img,wbr"),yC=gr("colgroup,dd,dt,li,p,tbody,td,tfoot,th,thead,tr"),xC=gr("rp,rt"),YT=qc(xC,yC),ZT=qc(yC,gr("address,article,aside,blockquote,caption,center,del,details,dialog,dir,div,dl,figure,figcaption,footer,h1,h2,h3,h4,h5,h6,header,hgroup,hr,ins,main,map,menu,nav,ol,pre,section,summary,table,ul")),QT=qc(xC,gr("a,abbr,acronym,audio,b,bdi,bdo,big,br,cite,code,del,dfn,em,font,i,img,ins,kbd,label,map,mark,picture,q,ruby,rp,rt,s,samp,small,source,span,strike,strong,sub,sup,time,track,tt,u,var,video")),vx=qc(bC,ZT,QT,YT),CC=gr("background,cite,href,itemtype,longdesc,poster,src,xlink:href"),XT=gr("abbr,accesskey,align,alt,autoplay,axis,bgcolor,border,cellpadding,cellspacing,class,clear,color,cols,colspan,compact,controls,coords,datetime,default,dir,download,face,headers,height,hidden,hreflang,hspace,ismap,itemscope,itemprop,kind,label,lang,language,loop,media,muted,nohref,nowrap,open,preload,rel,rev,role,rows,rowspan,rules,scope,scrolling,shape,size,sizes,span,srclang,srcset,start,summary,tabindex,target,title,translate,type,usemap,valign,value,vspace,width"),KT=gr("aria-activedescendant,aria-atomic,aria-autocomplete,aria-busy,aria-checked,aria-colcount,aria-colindex,aria-colspan,aria-controls,aria-current,aria-describedby,aria-details,aria-disabled,aria-dropeffect,aria-errormessage,aria-expanded,aria-flowto,aria-grabbed,aria-haspopup,aria-hidden,aria-invalid,aria-keyshortcuts,aria-label,aria-labelledby,aria-level,aria-live,aria-modal,aria-multiline,aria-multiselectable,aria-orientation,aria-owns,aria-placeholder,aria-posinset,aria-pressed,aria-readonly,aria-relevant,aria-required,aria-roledescription,aria-rowcount,aria-rowindex,aria-rowspan,aria-selected,aria-setsize,aria-sort,aria-valuemax,aria-valuemin,aria-valuenow,aria-valuetext"),JT=qc(CC,XT,KT),eA=gr("script,style,template"),Zg=class{sanitizedSomething=!1;buf=[];sanitizeChildren(n){let e=n.firstChild,i=!0,r=[];for(;e;){if(e.nodeType===Node.ELEMENT_NODE?i=this.startElement(e):e.nodeType===Node.TEXT_NODE?this.chars(e.nodeValue):this.sanitizedSomething=!0,i&&e.firstChild){r.push(e),e=iA(e);continue}for(;e;){e.nodeType===Node.ELEMENT_NODE&&this.endElement(e);let o=nA(e);if(o){e=o;break}e=r.pop()}}return this.buf.join("")}startElement(n){let e=bx(n).toLowerCase();if(!vx.hasOwnProperty(e))return this.sanitizedSomething=!0,!eA.hasOwnProperty(e);this.buf.push("<"),this.buf.push(e);let i=n.attributes;for(let r=0;r<i.length;r++){let o=i.item(r),s=o.name,a=s.toLowerCase();if(!JT.hasOwnProperty(a)){this.sanitizedSomething=!0;continue}let c=o.value;CC[a]&&(c=Wc(c)),this.buf.push(" ",s,'="',yx(c),'"')}return this.buf.push(">"),!0}endElement(n){let e=bx(n).toLowerCase();vx.hasOwnProperty(e)&&!bC.hasOwnProperty(e)&&(this.buf.push("</"),this.buf.push(e),this.buf.push(">"))}chars(n){this.buf.push(yx(n))}};function tA(t,n){return(t.compareDocumentPosition(n)&Node.DOCUMENT_POSITION_CONTAINED_BY)!==Node.DOCUMENT_POSITION_CONTAINED_BY}function nA(t){let n=t.nextSibling;if(n&&t!==n.previousSibling)throw wC(n);return n}function iA(t){let n=t.firstChild;if(n&&tA(t,n))throw wC(n);return n}function bx(t){let n=t.nodeName;return typeof n=="string"?n:"FORM"}function wC(t){return new Error(`Failed to sanitize html because the element is clobbered: ${t.outerHTML}`)}var rA=/[\uD800-\uDBFF][\uDC00-\uDFFF]/g,oA=/([^\#-~ |!])/g;function yx(t){return t.replace(/&/g,"&amp;").replace(rA,function(n){let e=n.charCodeAt(0),i=n.charCodeAt(1);return"&#"+((e-55296)*1024+(i-56320)+65536)+";"}).replace(oA,function(n){return"&#"+n.charCodeAt(0)+";"}).replace(/</g,"&lt;").replace(/>/g,"&gt;")}var Au;function k_(t,n){let e=null;try{Au=Au||GT(t);let i=n?String(n):"";e=Au.getInertBodyElement(i);let r=5,o=i;do{if(r===0)throw new Error("Failed to sanitize html because the input is unstable");r--,i=o,o=e.innerHTML,e=Au.getInertBodyElement(i)}while(i!==o);let a=new Zg().sanitizeChildren(xx(e)||e);return im(a)}finally{if(e){let i=xx(e)||e;for(;i.firstChild;)i.firstChild.remove()}}}function xx(t){return"content"in t&&sA(t)?t.content:null}function sA(t){return t.nodeType===Node.ELEMENT_NODE&&t.nodeName==="TEMPLATE"}var aA=/^>|^->|<!--|-->|--!>|<!-$/g,cA=/(<|>)/g,lA="\u200B$1\u200B";function dA(t){return t.replace(aA,n=>n.replace(cA,lA))}function uA(t,n){return t.createText(n)}function mA(t,n,e){t.setValue(n,e)}function fA(t,n){return t.createComment(dA(n))}function DC(t,n,e){return t.createElement(n,e)}function zu(t,n,e,i,r){t.insertBefore(n,e,i,r)}function EC(t,n,e){t.appendChild(n,e)}function Cx(t,n,e,i,r){i!==null?zu(t,n,e,i,r):EC(t,n,e)}function SC(t,n,e,i){t.removeChild(null,n,e,i)}function hA(t,n,e){t.setAttribute(n,"style",e)}function pA(t,n,e){e===""?t.removeAttribute(n,"class"):t.setAttribute(n,"class",e)}function IC(t,n,e){let{mergedAttrs:i,classes:r,styles:o}=e;i!==null&&xT(t,n,i),r!==null&&pA(t,n,r),o!==null&&hA(t,n,o)}var Bt=(function(t){return t[t.NONE=0]="NONE",t[t.HTML=1]="HTML",t[t.STYLE=2]="STYLE",t[t.SCRIPT=3]="SCRIPT",t[t.URL=4]="URL",t[t.RESOURCE_URL=5]="RESOURCE_URL",t[t.ATTRIBUTE_NO_BINDING=6]="ATTRIBUTE_NO_BINDING",t})(Bt||{});function Qn(t){let n=MC();return n?n.sanitize(Bt.URL,t)||"":pr(t,"URL")?gi(t):Wc(Ho(t))}function kC(t){let n=MC();if(n)return _x(n.sanitize(Bt.RESOURCE_URL,t)||"");if(pr(t,"ResourceURL"))return _x(gi(t));throw new R(904,!1)}var gA={embed:{src:!0},frame:{src:!0},iframe:{src:!0},media:{src:!0},base:{href:!0},link:{href:!0},object:{data:!0,codebase:!0}};function _A(t,n){return gA[t.toLowerCase()]?.[n.toLowerCase()]===!0?kC:Qn}function M_(t,n,e){return _A(n,e)(t)}function MC(){let t=ne();return t&&t[ui].sanitizer}function TC(t){return t instanceof Function?t():t}function vA(t,n,e){let i=t.length;for(;;){let r=t.indexOf(n,e);if(r===-1)return r;if(r===0||t.charCodeAt(r-1)<=32){let o=n.length;if(r+o===i||t.charCodeAt(r+o)<=32)return r}e=r+1}}var AC="ng-template";function bA(t,n,e,i){let r=0;if(i){for(;r<n.length&&typeof n[r]=="string";r+=2)if(n[r]==="class"&&vA(n[r+1].toLowerCase(),e,0)!==-1)return!0}else if(T_(t))return!1;if(r=n.indexOf(1,r),r>-1){let o;for(;++r<n.length&&typeof(o=n[r])=="string";)if(o.toLowerCase()===e)return!0}return!1}function T_(t){return t.type===4&&t.value!==AC}function yA(t,n,e){let i=t.type===4&&!e?AC:t.value;return n===i}function xA(t,n,e){let i=4,r=t.attrs,o=r!==null?DA(r):0,s=!1;for(let a=0;a<n.length;a++){let c=n[a];if(typeof c=="number"){if(!s&&!fi(i)&&!fi(c))return!1;if(s&&fi(c))continue;s=!1,i=c|i&1;continue}if(!s)if(i&4){if(i=2|i&1,c!==""&&!yA(t,c,e)||c===""&&n.length===1){if(fi(i))return!1;s=!0}}else if(i&8){if(r===null||!bA(t,r,c,e)){if(fi(i))return!1;s=!0}}else{let l=n[++a],u=CA(c,r,T_(t),e);if(u===-1){if(fi(i))return!1;s=!0;continue}if(l!==""){let m;if(u>o?m="":m=r[u+1].toLowerCase(),i&2&&l!==m){if(fi(i))return!1;s=!0}}}}return fi(i)||s}function fi(t){return(t&1)===0}function CA(t,n,e,i){if(n===null)return-1;let r=0;if(i||!e){let o=!1;for(;r<n.length;){let s=n[r];if(s===t)return r;if(s===3||s===6)o=!0;else if(s===1||s===2){let a=n[++r];for(;typeof a=="string";)a=n[++r];continue}else{if(s===4)break;if(s===0){r+=4;continue}}r+=o?1:2}return-1}else return EA(n,t)}function RC(t,n,e=!1){for(let i=0;i<n.length;i++)if(xA(t,n[i],e))return!0;return!1}function wA(t){let n=t.attrs;if(n!=null){let e=n.indexOf(5);if((e&1)===0)return n[e+1]}return null}function DA(t){for(let n=0;n<t.length;n++){let e=t[n];if(Qx(e))return n}return t.length}function EA(t,n){let e=t.indexOf(4);if(e>-1)for(e++;e<t.length;){let i=t[e];if(typeof i=="number")return-1;if(i===n)return e;e++}return-1}function SA(t,n){e:for(let e=0;e<n.length;e++){let i=n[e];if(t.length===i.length){for(let r=0;r<t.length;r++)if(t[r]!==i[r])continue e;return!0}}return!1}function wx(t,n){return t?":not("+n.trim()+")":n}function IA(t){let n=t[0],e=1,i=2,r="",o=!1;for(;e<t.length;){let s=t[e];if(typeof s=="string")if(i&2){let a=t[++e];r+="["+s+(a.length>0?'="'+a+'"':"")+"]"}else i&8?r+="."+s:i&4&&(r+=" "+s);else r!==""&&!fi(s)&&(n+=wx(o,r),r=""),i=s,o=o||!fi(i);e++}return r!==""&&(n+=wx(o,r)),n}function kA(t){return t.map(IA).join(",")}function MA(t){let n=[],e=[],i=1,r=2;for(;i<t.length;){let o=t[i];if(typeof o=="string")r===2?o!==""&&n.push(o,t[++i]):r===8&&e.push(o);else{if(!fi(r))break;r=o}i++}return e.length&&n.push(1,...e),n}var _n={};function A_(t,n,e,i,r,o,s,a,c,l,u){let m=at+i,g=m+r,v=TA(m,g),x=typeof l=="function"?l():l;return v[ie]={type:t,blueprint:v,template:e,queries:null,viewQuery:a,declTNode:n,data:v.slice().fill(null,m),bindingStartIndex:m,expandoStartIndex:g,hostBindingOpCodes:null,firstCreatePass:!0,firstUpdatePass:!0,staticViewQueries:!1,staticContentQueries:!1,preOrderHooks:null,preOrderCheckHooks:null,contentHooks:null,contentCheckHooks:null,viewHooks:null,viewCheckHooks:null,destroyHooks:null,cleanup:null,contentQueries:null,components:null,directiveRegistry:typeof o=="function"?o():o,pipeRegistry:typeof s=="function"?s():s,firstChild:null,schemas:c,consts:x,incompleteFirstPass:!1,ssrId:u}}function TA(t,n){let e=[];for(let i=0;i<n;i++)e.push(i<t?null:_n);return e}function AA(t){let n=t.tView;return n===null||n.incompleteFirstPass?t.tView=A_(1,null,t.template,t.decls,t.vars,t.directiveDefs,t.pipeDefs,t.viewQuery,t.schemas,t.consts,t.id):n}function R_(t,n,e,i,r,o,s,a,c,l,u){let m=n.blueprint.slice();return m[di]=r,m[se]=i|4|128|8|64|1024,(l!==null||t&&t[se]&2048)&&(m[se]|=2048),rg(m),m[Lt]=m[zo]=t,m[kt]=e,m[ui]=s||t&&t[ui],m[Ue]=a||t&&t[Ue],m[lr]=c||t&&t[lr]||null,m[hn]=o,m[dr]=PT(),m[Uo]=u,m[Kp]=l,m[pn]=n.type==2?t[pn]:m,m}function RA(t,n,e){let i=Yn(n,t),r=AA(e),o=t[ui].rendererFactory,s=O_(t,R_(t,r,null,OC(e),i,n,null,o.createRenderer(i,e),null,null,null));return t[n.index]=s}function OC(t){let n=16;return t.signals?n=4096:t.onPush&&(n=64),n}function NC(t,n,e,i){if(e===0)return-1;let r=n.length;for(let o=0;o<e;o++)n.push(i),t.blueprint.push(i),t.data.push(null);return r}function O_(t,n){return t[Vs]?t[Xp][Wn]=n:t[Vs]=n,t[Xp]=n,n}function p(t=1){PC(Qe(),ne(),Bi()+t,!1)}function PC(t,n,e,i){if(!i)if((n[se]&3)===3){let o=t.preOrderCheckHooks;o!==null&&Ou(n,o,e)}else{let o=t.preOrderHooks;o!==null&&Nu(n,o,0,e)}Wr(e)}var rm=(function(t){return t[t.None=0]="None",t[t.SignalBased=1]="SignalBased",t[t.HasDecoratorInputTransform=2]="HasDecoratorInputTransform",t})(rm||{});function Qg(t,n,e,i){let r=te(null);try{let[o,s,a]=t.inputs[e],c=null;(s&rm.SignalBased)!==0&&(c=n[o][Et]),c!==null&&c.transformFn!==void 0?i=c.transformFn(i):a!==null&&(i=a.call(n,i)),t.setInput!==null?t.setInput(n,c,i,e,o):$x(n,c,o,i)}finally{te(r)}}var Ui=(function(t){return t[t.Important=1]="Important",t[t.DashCase=2]="DashCase",t})(Ui||{}),OA;function N_(t,n){return OA(t,n)}var v6=typeof document<"u"&&typeof document?.documentElement?.getAnimations=="function";var Xg=new WeakMap,Nc=new WeakSet;function NA(t,n){let e=Xg.get(t);if(!e||e.length===0)return;let i=n.parentNode,r=n.previousSibling;for(let o=e.length-1;o>=0;o--){let s=e[o],a=s.parentNode;s===n?(e.splice(o,1),Nc.add(s),s.dispatchEvent(new CustomEvent("animationend",{detail:{cancel:!0}}))):(r&&s===r||a&&i&&a!==i)&&(e.splice(o,1),s.dispatchEvent(new CustomEvent("animationend",{detail:{cancel:!0}})),s.parentNode?.removeChild(s))}}function PA(t,n){let e=Xg.get(t);e?e.includes(n)||e.push(n):Xg.set(t,[n])}var Xo=new Set,om=(function(t){return t[t.CHANGE_DETECTION=0]="CHANGE_DETECTION",t[t.AFTER_NEXT_RENDER=1]="AFTER_NEXT_RENDER",t})(om||{}),_i=new y(""),Dx=new Set;function Zr(t){Dx.has(t)||(Dx.add(t),performance?.mark?.("mark_feature_usage",{detail:{feature:t}}))}var sm=(()=>{class t{impl=null;execute(){this.impl?.execute()}static \u0275prov=C({token:t,providedIn:"root",factory:()=>new t})}return t})(),P_=[0,1,2,3],F_=(()=>{class t{ngZone=d(H);scheduler=d(Ri);errorHandler=d(rn,{optional:!0});sequences=new Set;deferredRegistrations=new Set;executing=!1;constructor(){d(_i,{optional:!0})}execute(){let e=this.sequences.size>0;e&&Be(Me.AfterRenderHooksStart),this.executing=!0;for(let i of P_)for(let r of this.sequences)if(!(r.erroredOrDestroyed||!r.hooks[i]))try{r.pipelinedValue=this.ngZone.runOutsideAngular(()=>this.maybeTrace(()=>{let o=r.hooks[i];return o(r.pipelinedValue)},r.snapshot))}catch(o){r.erroredOrDestroyed=!0,this.errorHandler?.handleError(o)}this.executing=!1;for(let i of this.sequences)i.afterRun(),i.once&&(this.sequences.delete(i),i.destroy());for(let i of this.deferredRegistrations)this.sequences.add(i);this.deferredRegistrations.size>0&&this.scheduler.notify(7),this.deferredRegistrations.clear(),e&&Be(Me.AfterRenderHooksEnd)}register(e){let{view:i}=e;i!==void 0?((i[Go]??=[]).push(e),Yo(i),i[se]|=8192):this.executing?this.deferredRegistrations.add(e):this.addSequence(e)}addSequence(e){this.sequences.add(e),this.scheduler.notify(7)}unregister(e){this.executing&&this.sequences.has(e)?(e.erroredOrDestroyed=!0,e.pipelinedValue=void 0,e.once=!0):(this.sequences.delete(e),this.deferredRegistrations.delete(e))}maybeTrace(e,i){return i?i.run(om.AFTER_NEXT_RENDER,e):e()}static \u0275prov=C({token:t,providedIn:"root",factory:()=>new t})}return t})(),jc=class{impl;hooks;view;once;snapshot;erroredOrDestroyed=!1;pipelinedValue=void 0;unregisterOnDestroy;constructor(n,e,i,r,o,s=null){this.impl=n,this.hooks=e,this.view=i,this.once=r,this.snapshot=s,this.unregisterOnDestroy=o?.onDestroy(()=>this.destroy())}afterRun(){this.erroredOrDestroyed=!1,this.pipelinedValue=void 0,this.snapshot?.dispose(),this.snapshot=null}destroy(){this.impl.unregister(this),this.unregisterOnDestroy?.();let n=this.view?.[Go];n&&(this.view[Go]=n.filter(e=>e!==this))}};function bt(t,n){let e=n?.injector??d(X);return Zr("NgAfterNextRender"),LA(t,e,n,!0)}function FA(t){return t instanceof Function?[void 0,void 0,t,void 0]:[t.earlyRead,t.write,t.mixedReadWrite,t.read]}function LA(t,n,e,i){let r=n.get(sm);r.impl??=n.get(F_);let o=n.get(_i,null,{optional:!0}),s=e?.manualCleanup!==!0?n.get(on):null,a=n.get(Gs,null,{optional:!0}),c=new jc(r.impl,FA(t),a?.view,i,s,o?.snapshot(null));return r.impl.register(c),c}var FC=new y("",{factory:()=>({queue:new Set,isScheduled:!1,scheduler:null,injector:d(He)})});function LC(t,n,e){let i=t.get(FC);if(Array.isArray(n))for(let r of n)i.queue.add(r),e?.detachedLeaveAnimationFns?.push(r);else i.queue.add(n),e?.detachedLeaveAnimationFns?.push(n);i.scheduler&&i.scheduler(t)}function jA(t,n){let e=t.get(FC);if(n.detachedLeaveAnimationFns){for(let i of n.detachedLeaveAnimationFns)e.queue.delete(i);n.detachedLeaveAnimationFns=void 0}}function BA(t,n){for(let[e,i]of n)LC(t,i.animateFns)}function Ex(t,n,e,i){let r=t?.[$r]?.enter;n!==null&&r&&r.has(e.index)&&BA(i,r)}function Ws(t,n,e,i,r,o,s,a){if(r!=null){let c,l=!1;mi(r)?c=r:ur(r)&&(l=!0,r=r[di]);let u=qn(r);t===0&&i!==null?(Ex(a,i,o,e),s==null?EC(n,i,u):zu(n,i,u,s||null,!0)):t===1&&i!==null?(Ex(a,i,o,e),zu(n,i,u,s||null,!0),NA(o,u)):t===2?(a?.[$r]?.leave?.has(o.index)&&PA(o,u),Nc.delete(u),Sx(a,o,e,m=>{if(Nc.has(u)){Nc.delete(u);return}SC(n,u,l,m)})):t===3&&(Nc.delete(u),Sx(a,o,e,()=>{n.destroyNode(u)})),c!=null&&QA(n,t,e,c,o,i,s)}}function VA(t,n){jC(t,n),n[di]=null,n[hn]=null}function HA(t,n,e,i,r,o){i[di]=r,i[hn]=n,cm(t,i,e,1,r,o)}function jC(t,n){n[ui].changeDetectionScheduler?.notify(9),cm(t,n,n[Ue],2,null,null)}function UA(t){let n=t[Vs];if(!n)return kg(t[ie],t);for(;n;){let e=null;if(ur(n))e=n[Vs];else{let i=n[_t];i&&(e=i)}if(!e){for(;n&&!n[Wn]&&n!==t;)ur(n)&&kg(n[ie],n),n=n[Lt];n===null&&(n=t),ur(n)&&kg(n[ie],n),e=n&&n[Wn]}n=e}}function L_(t,n){let e=t[Wo],i=e.indexOf(n);e.splice(i,1)}function am(t,n){if(qo(n))return;let e=n[Ue];e.destroyNode&&cm(t,n,e,3,null,null),UA(n)}function kg(t,n){if(qo(n))return;let e=te(null);try{n[se]&=-129,n[se]|=256,n[Nn]&&Pr(n[Nn]),GA(t,n),$A(t,n),n[ie].type===1&&n[Ue].destroy();let i=n[zr];if(i!==null&&mi(n[Lt])){i!==n[Lt]&&L_(i,n);let r=n[Pi];r!==null&&r.detachView(t)}Vg(n)}finally{te(e)}}function Sx(t,n,e,i){let r=t?.[$r];if(r==null||r.leave==null||!r.leave.has(n.index))return i(!1);t&&Xo.add(t[dr]),LC(e,()=>{if(r.leave&&r.leave.has(n.index)){let s=r.leave.get(n.index),a=[];if(s){for(let c=0;c<s.animateFns.length;c++){let l=s.animateFns[c],{promise:u}=l();a.push(u)}r.detachedLeaveAnimationFns=void 0}r.running=Promise.allSettled(a),zA(t,i)}else t&&Xo.delete(t[dr]),i(!1)},r)}function zA(t,n){let e=t[$r]?.running;if(e){e.then(()=>{t[$r].running=void 0,Xo.delete(t[dr]),n(!0)});return}n(!1)}function $A(t,n){let e=t.cleanup,i=n[Bs];if(e!==null)for(let s=0;s<e.length-1;s+=2)if(typeof e[s]=="string"){let a=e[s+3];a>=0?i[a]():i[-a].unsubscribe(),s+=2}else{let a=i[e[s+1]];e[s].call(a)}i!==null&&(n[Bs]=null);let r=n[ar];if(r!==null){n[ar]=null;for(let s=0;s<r.length;s++){let a=r[s];a()}}let o=n[Br];if(o!==null){n[Br]=null;for(let s of o)s.destroy()}}function GA(t,n){let e;if(t!=null&&(e=t.destroyHooks)!=null)for(let i=0;i<e.length;i+=2){let r=n[e[i]];if(!(r instanceof Qo)){let o=e[i+1];if(Array.isArray(o))for(let s=0;s<o.length;s+=2){let a=r[o[s]],c=o[s+1];Be(Me.LifecycleHookStart,a,c);try{c.call(a)}finally{Be(Me.LifecycleHookEnd,a,c)}}else{Be(Me.LifecycleHookStart,r,o);try{o.call(r)}finally{Be(Me.LifecycleHookEnd,r,o)}}}}}function BC(t,n,e){return WA(t,n.parent,e)}function WA(t,n,e){let i=n;for(;i!==null&&i.type&168;)n=i,i=n.parent;if(i===null)return e[di];if(Fi(i)){let{encapsulation:r}=t.data[i.directiveStart+i.componentOffset];if(r===pi.None||r===pi.Emulated)return null}return Yn(i,e)}function VC(t,n,e){return YA(t,n,e)}function qA(t,n,e){return t.type&40?Yn(t,e):null}var YA=qA,Ix;function j_(t,n,e,i){let r=BC(t,i,n),o=n[Ue],s=i.parent||n[hn],a=VC(s,i,n);if(r!=null)if(Array.isArray(e))for(let c=0;c<e.length;c++)Cx(o,r,e[c],a,!1);else Cx(o,r,e,a,!1);Ix!==void 0&&Ix(o,i,n,e,r)}function Pc(t,n){if(n!==null){let e=n.type;if(e&3)return Yn(n,t);if(e&4)return Kg(-1,t[n.index]);if(e&8){let i=n.child;if(i!==null)return Pc(t,i);{let r=t[n.index];return mi(r)?Kg(-1,r):qn(r)}}else{if(e&128)return Pc(t,n.next);if(e&32)return N_(n,t)()||qn(t[n.index]);{let i=HC(t,n);if(i!==null){if(Array.isArray(i))return i[0];let r=Vr(t[pn]);return Pc(r,i)}else return Pc(t,n.next)}}}return null}function HC(t,n){if(n!==null){let i=t[pn][hn],r=n.projection;return i.projection[r]}return null}function Kg(t,n){let e=_t+t+1;if(e<n.length){let i=n[e],r=i[ie].firstChild;if(r!==null)return Pc(i,r)}return n[Gr]}function B_(t,n,e,i,r,o,s){for(;e!=null;){let a=i[lr];if(e.type===128){e=e.next;continue}let c=i[e.index],l=e.type;if(s&&n===0&&(c&&Zs(qn(c),i),e.flags|=2),!nm(e))if(l&8)B_(t,n,e.child,i,r,o,!1),Ws(n,t,a,r,c,e,o,i);else if(l&32){let u=N_(e,i),m;for(;m=u();)Ws(n,t,a,r,m,e,o,i);Ws(n,t,a,r,c,e,o,i)}else l&16?UC(t,n,i,e,r,o):Ws(n,t,a,r,c,e,o,i);e=s?e.projectionNext:e.next}}function cm(t,n,e,i,r,o){B_(e,i,t.firstChild,n,r,o,!1)}function ZA(t,n,e){let i=n[Ue],r=BC(t,e,n),o=e.parent||n[hn],s=VC(o,e,n);UC(i,0,n,e,r,s)}function UC(t,n,e,i,r,o){let s=e[pn],c=s[hn].projection[i.projection];if(Array.isArray(c))for(let l=0;l<c.length;l++){let u=c[l];Ws(n,t,e[lr],r,u,i,o,e)}else{let l=c,u=s[Lt];aC(i)&&(l.flags|=128),B_(t,n,l,u,r,o,!0)}}function QA(t,n,e,i,r,o,s){let a=i[Gr],c=qn(i);a!==c&&Ws(n,t,e,o,a,r,s);for(let l=_t;l<i.length;l++){let u=i[l];cm(u[ie],u,t,n,o,a)}}function XA(t,n,e,i,r){if(n)r?t.addClass(e,i):t.removeClass(e,i);else{let o=i.indexOf("-")===-1?void 0:Ui.DashCase;r==null?t.removeStyle(e,i,o):(typeof r=="string"&&r.endsWith("!important")&&(r=r.slice(0,-10),o|=Ui.Important),t.setStyle(e,i,r,o))}}function zC(t,n,e,i,r){let o=Bi(),s=i&2;try{Wr(-1),s&&n.length>at&&PC(t,n,at,!1);let a=s?Me.TemplateUpdateStart:Me.TemplateCreateStart;Be(a,r,e),e(i,r)}finally{Wr(o);let a=s?Me.TemplateUpdateEnd:Me.TemplateCreateEnd;Be(a,r,e)}}function lm(t,n,e){nR(t,n,e),(e.flags&64)===64&&iR(t,n,e)}function Yc(t,n,e=Yn){let i=n.localNames;if(i!==null){let r=n.index+1;for(let o=0;o<i.length;o+=2){let s=i[o+1],a=s===-1?e(n,t):t[s];t[r++]=a}}}function KA(t,n,e,i){let o=i.get(pC,hC)||e===pi.ShadowDom||e===pi.ExperimentalIsolatedShadowDom,s=t.selectRootElement(n,o);if(s.tagName.toLowerCase()==="script")throw new R(905,!1);return JA(s),s}function JA(t){eR(t)}var eR=()=>null;function tR(t){return t==="class"?"className":t==="for"?"htmlFor":t==="formaction"?"formAction":t==="innerHtml"?"innerHTML":t==="readonly"?"readOnly":t==="tabindex"?"tabIndex":t}function $C(t,n,e,i,r,o){let s=n[ie];if(dm(t,s,n,e,i)){Fi(t)&&WC(n,t.index);return}t.type&3&&(e=tR(e)),GC(t,n,e,i,r,o)}function GC(t,n,e,i,r,o){if(t.type&3){let s=Yn(t,n);i=o!=null?o(i,t.value||"",e):i,r.setProperty(s,e,i)}else t.type&12}function WC(t,n){let e=Zn(n,t);e[se]&16||(e[se]|=64)}function nR(t,n,e){let i=e.directiveStart,r=e.directiveEnd;Fi(e)&&RA(n,e,t.data[i+e.componentOffset]),t.firstCreatePass||Uu(e,n);let o=e.initialInputs;for(let s=i;s<r;s++){let a=t.data[s],c=Lc(n,t,s,e);if(Zs(c,n),o!==null&&sR(n,s-i,c,a,e,o),Li(a)){let l=Zn(e.index,n);l[kt]=Lc(n,t,s,e)}}}function iR(t,n,e){let i=e.directiveStart,r=e.directiveEnd,o=e.index,s=Q0();try{Wr(o);for(let a=i;a<r;a++){let c=t.data[a],l=n[a];Cu(a),(c.hostBindings!==null||c.hostVars!==0||c.hostAttrs!==null)&&rR(c,l)}}finally{Wr(-1),Cu(s)}}function rR(t,n){t.hostBindings!==null&&t.hostBindings(1,n)}function V_(t,n){let e=t.directiveRegistry,i=null;if(e)for(let r=0;r<e.length;r++){let o=e[r];RC(n,o.selectors,!1)&&(i??=[],Li(o)?i.unshift(o):i.push(o))}return i}function oR(t,n,e,i,r,o){let s=Yn(t,n);qC(n[Ue],s,o,t.value,e,i,r)}function qC(t,n,e,i,r,o,s){if(o==null)t.removeAttribute(n,r,e);else{let a=s==null?Ho(o):s(o,i||"",r);t.setAttribute(n,r,a,e)}}function sR(t,n,e,i,r,o){let s=o[n];if(s!==null)for(let a=0;a<s.length;a+=2){let c=s[a],l=s[a+1];Qg(i,e,c,l)}}function H_(t,n,e,i,r){let o=at+e,s=n[ie],a=r(s,n,t,i,e);n[o]=a,zs(t,!0);let c=t.type===2;return c?(IC(n[Ue],a,t),(U0()===0||Hs(t))&&Zs(a,n),z0()):Zs(a,n),Su()&&(!c||!nm(t))&&j_(s,n,a,t),t}function U_(t){let n=t;return hg()?pg():(n=n.parent,zs(n,!1)),n}function aR(t,n){let e=t[lr];if(!e)return;let i;try{i=e.get(Fn,null)}catch{i=null}i?.(n)}function dm(t,n,e,i,r){let o=t.inputs?.[i],s=t.hostDirectiveInputs?.[i],a=!1;if(s)for(let c=0;c<s.length;c+=2){let l=s[c],u=s[c+1],m=n.data[l];Qg(m,e[l],u,r),a=!0}if(o)for(let c of o){let l=e[c],u=n.data[c];Qg(u,l,i,r),a=!0}return a}function cR(t,n){let e=Zn(n,t),i=e[ie];lR(i,e);let r=e[di];r!==null&&e[Uo]===null&&(e[Uo]=gC(r,e[lr])),Be(Me.ComponentStart);try{z_(i,e,e[kt])}finally{Be(Me.ComponentEnd,e[kt])}}function lR(t,n){for(let e=n.length;e<t.blueprint.length;e++)n.push(t.blueprint[e])}function z_(t,n,e){Du(n);try{let i=t.viewQuery;i!==null&&Hg(1,i,e);let r=t.template;r!==null&&zC(t,n,r,1,e),t.firstCreatePass&&(t.firstCreatePass=!1),n[Pi]?.finishViewCreation(t),t.staticContentQueries&&_C(t,n),t.staticViewQueries&&Hg(2,t.viewQuery,e);let o=t.components;o!==null&&dR(n,o)}catch(i){throw t.firstCreatePass&&(t.incompleteFirstPass=!0,t.firstCreatePass=!1),i}finally{n[se]&=-5,Eu()}}function dR(t,n){for(let e=0;e<n.length;e++)cR(t,n[e])}function Zc(t,n,e,i){let r=te(null);try{let o=n.tView,a=t[se]&4096?4096:16,c=R_(t,o,e,a,null,n,null,null,i?.injector??null,i?.embeddedViewInjector??null,i?.dehydratedView??null),l=t[n.index];c[zr]=l;let u=t[Pi];return u!==null&&(c[Pi]=u.createEmbeddedView(o)),z_(o,c,e),c}finally{te(r)}}function Qs(t,n){return!n||n.firstChild===null||aC(t)}function Bc(t,n,e,i,r=!1){for(;e!==null;){if(e.type===128){e=r?e.projectionNext:e.next;continue}let o=n[e.index];o!==null&&i.push(qn(o)),mi(o)&&YC(o,i);let s=e.type;if(s&8)Bc(t,n,e.child,i);else if(s&32){let a=N_(e,n),c;for(;c=a();)i.push(c)}else if(s&16){let a=HC(n,e);if(Array.isArray(a))i.push(...a);else{let c=Vr(n[pn]);Bc(c[ie],c,a,i,!0)}}e=r?e.projectionNext:e.next}return i}function YC(t,n){for(let e=_t;e<t.length;e++){let i=t[e],r=i[ie].firstChild;r!==null&&Bc(i[ie],i,r,n)}t[Gr]!==t[di]&&n.push(t[Gr])}function ZC(t){if(t[Go]!==null){for(let n of t[Go])n.impl.addSequence(n);t[Go].length=0}}var QC=[];function uR(t){return t[Nn]??mR(t)}function mR(t){let n=QC.pop()??Object.create(hR);return n.lView=t,n}function fR(t){t.lView[Nn]!==t&&(t.lView=null,QC.push(t))}var hR=Z(b({},Rr),{consumerIsAlwaysLive:!0,kind:"template",consumerMarkedDirty:t=>{Yo(t.lView)},consumerOnSignalRead(){this.lView[Nn]=this}});function pR(t){let n=t[Nn]??Object.create(gR);return n.lView=t,n}var gR=Z(b({},Rr),{consumerIsAlwaysLive:!0,kind:"template",consumerMarkedDirty:t=>{let n=Vr(t.lView);for(;n&&!XC(n[ie]);)n=Vr(n);n&&og(n)},consumerOnSignalRead(){this.lView[Nn]=this}});function XC(t){return t.type!==2}function KC(t){if(t[Br]===null)return;let n=!0;for(;n;){let e=!1;for(let i of t[Br])i.dirty&&(e=!0,i.zone===null||Zone.current===i.zone?i.run():i.zone.run(()=>i.run()));n=e&&!!(t[se]&8192)}}var _R=100;function JC(t,n=0){let i=t[ui].rendererFactory,r=!1;r||i.begin?.();try{vR(t,n)}finally{r||i.end?.()}}function vR(t,n){let e=gg();try{_c(!0),Jg(t,n);let i=0;for(;Ic(t);){if(i===_R)throw new R(103,!1);i++,Jg(t,1)}}finally{_c(e)}}function bR(t,n,e,i){if(qo(n))return;let r=n[se],o=!1,s=!1;Du(n);let a=!0,c=null,l=null;o||(XC(t)?(l=uR(n),c=rr(l)):wd()===null?(a=!1,l=pR(n),c=rr(l)):n[Nn]&&(Pr(n[Nn]),n[Nn]=null));try{rg(n),q0(t.bindingStartIndex),e!==null&&zC(t,n,e,2,i);let u=(r&3)===3;if(!o)if(u){let v=t.preOrderCheckHooks;v!==null&&Ou(n,v,null)}else{let v=t.preOrderHooks;v!==null&&Nu(n,v,0,null),Sg(n,0)}if(s||yR(n),KC(n),ew(n,0),t.contentQueries!==null&&_C(t,n),!o)if(u){let v=t.contentCheckHooks;v!==null&&Ou(n,v)}else{let v=t.contentHooks;v!==null&&Nu(n,v,1),Sg(n,1)}CR(t,n);let m=t.components;m!==null&&nw(n,m,0);let g=t.viewQuery;if(g!==null&&Hg(2,g,i),!o)if(u){let v=t.viewCheckHooks;v!==null&&Ou(n,v)}else{let v=t.viewHooks;v!==null&&Nu(n,v,2),Sg(n,2)}if(t.firstUpdatePass===!0&&(t.firstUpdatePass=!1),n[pu]){for(let v of n[pu])v();n[pu]=null}o||(ZC(n),n[se]&=-73)}catch(u){throw o||Yo(n),u}finally{l!==null&&(Nr(l,c),a&&fR(l)),Eu()}}function ew(t,n){for(let e=lC(t);e!==null;e=dC(e))for(let i=_t;i<e.length;i++){let r=e[i];tw(r,n)}}function yR(t){for(let n=lC(t);n!==null;n=dC(n)){if(!(n[se]&2))continue;let e=n[Wo];for(let i=0;i<e.length;i++){let r=e[i];og(r)}}}function xR(t,n,e){Be(Me.ComponentStart);let i=Zn(n,t);try{tw(i,e)}finally{Be(Me.ComponentEnd,i[kt])}}function tw(t,n){vu(t)&&Jg(t,n)}function Jg(t,n){let i=t[ie],r=t[se],o=t[Nn],s=!!(n===0&&r&16);if(s||=!!(r&64&&n===0),s||=!!(r&1024),s||=!!(o?.dirty&&ks(o)),s||=!1,o&&(o.dirty=!1),t[se]&=-9217,s)bR(i,t,i.template,t[kt]);else if(r&8192){let a=te(null);try{KC(t),ew(t,1);let c=i.components;c!==null&&nw(t,c,1),ZC(t)}finally{te(a)}}}function nw(t,n,e){for(let i=0;i<n.length;i++)xR(t,n[i],e)}function CR(t,n){let e=t.hostBindingOpCodes;if(e!==null)try{for(let i=0;i<e.length;i++){let r=e[i];if(r<0)Wr(~r);else{let o=r,s=e[++i],a=e[++i];Z0(s,o);let c=n[o];Be(Me.HostBindingsUpdateStart,c);try{a(2,c)}finally{Be(Me.HostBindingsUpdateEnd,c)}}}}finally{Wr(-1)}}function $_(t,n){let e=gg()?64:1088;for(t[ui].changeDetectionScheduler?.notify(n);t;){t[se]|=e;let i=Vr(t);if(Us(t)&&!i)return t;t=i}return null}function iw(t,n,e,i){return[t,!0,0,n,null,i,null,e,null,null]}function rw(t,n){let e=_t+n;if(e<t.length)return t[e]}function Qc(t,n,e,i=!0){let r=n[ie];if(wR(r,n,t,e),i){let s=Kg(e,t),a=n[Ue],c=a.parentNode(t[Gr]);c!==null&&HA(r,t[hn],a,n,c,s)}let o=n[Uo];o!==null&&o.firstChild!==null&&(o.firstChild=null)}function ow(t,n){let e=Vc(t,n);return e!==void 0&&am(e[ie],e),e}function Vc(t,n){if(t.length<=_t)return;let e=_t+n,i=t[e];if(i){let r=i[zr];r!==null&&r!==t&&L_(r,i),n>0&&(t[e-1][Wn]=i[Wn]);let o=Dc(t,_t+n);VA(i[ie],i);let s=o[Pi];s!==null&&s.detachView(o[ie]),i[Lt]=null,i[Wn]=null,i[se]&=-129}return i}function wR(t,n,e,i){let r=_t+i,o=e.length;i>0&&(e[r-1][Wn]=n),i<o-_t?(n[Wn]=e[r],Gp(e,_t+i,n)):(e.push(n),n[Wn]=null),n[Lt]=e;let s=n[zr];s!==null&&e!==s&&sw(s,n);let a=n[Pi];a!==null&&a.insertView(t),bu(n),n[se]|=128}function sw(t,n){let e=t[Wo],i=n[Lt];if(ur(i))t[se]|=2;else{let r=i[Lt][pn];n[pn]!==r&&(t[se]|=2)}e===null?t[Wo]=[n]:e.push(n)}var qr=class{_lView;_cdRefInjectingView;_appRef=null;_attachedToViewContainer=!1;exhaustive;get rootNodes(){let n=this._lView,e=n[ie];return Bc(e,n,e.firstChild,[])}constructor(n,e){this._lView=n,this._cdRefInjectingView=e}get context(){return this._lView[kt]}set context(n){this._lView[kt]=n}get destroyed(){return qo(this._lView)}destroy(){if(this._appRef)this._appRef.detachView(this);else if(this._attachedToViewContainer){let n=this._lView[Lt];if(mi(n)){let e=n[Sc],i=e?e.indexOf(this):-1;i>-1&&(Vc(n,i),Dc(e,i))}this._attachedToViewContainer=!1}am(this._lView[ie],this._lView)}onDestroy(n){sg(this._lView,n)}markForCheck(){$_(this._cdRefInjectingView||this._lView,4)}detach(){this._lView[se]&=-129}reattach(){bu(this._lView),this._lView[se]|=128}detectChanges(){this._lView[se]|=1024,JC(this._lView)}checkNoChanges(){}attachToViewContainerRef(){if(this._appRef)throw new R(902,!1);this._attachedToViewContainer=!0}detachFromAppRef(){this._appRef=null;let n=Us(this._lView),e=this._lView[zr];e!==null&&!n&&L_(e,this._lView),jC(this._lView[ie],this._lView)}attachToAppRef(n){if(this._attachedToViewContainer)throw new R(902,!1);this._appRef=n;let e=Us(this._lView),i=this._lView[zr];i!==null&&!e&&sw(i,this._lView),bu(this._lView)}};var vt=(()=>{class t{_declarationLView;_declarationTContainer;elementRef;static __NG_ELEMENT_ID__=DR;constructor(e,i,r){this._declarationLView=e,this._declarationTContainer=i,this.elementRef=r}get ssrId(){return this._declarationTContainer.tView?.ssrId||null}createEmbeddedView(e,i){return this.createEmbeddedViewImpl(e,i)}createEmbeddedViewImpl(e,i,r){let o=Zc(this._declarationLView,this._declarationTContainer,e,{embeddedViewInjector:i,dehydratedView:r});return new qr(o)}}return t})();function DR(){return um(jt(),ne())}function um(t,n){return t.type&4?new vt(n,t,Js(t,n)):null}function ea(t,n,e,i,r){let o=t.data[n];if(o===null)o=ER(t,n,e,i,r),Y0()&&(o.flags|=32);else if(o.type&64){o.type=e,o.value=i,o.attrs=r;let s=$0();o.injectorIndex=s===null?-1:s.injectorIndex}return zs(o,!0),o}function ER(t,n,e,i,r){let o=fg(),s=hg(),a=s?o:o&&o.parent,c=t.data[n]=IR(t,a,e,n,i,r);return SR(t,c,o,s),c}function SR(t,n,e,i){t.firstChild===null&&(t.firstChild=n),e!==null&&(i?e.child==null&&n.parent!==null&&(e.child=n):e.next===null&&(e.next=n,n.prev=e))}function IR(t,n,e,i,r,o){let s=n?n.injectorIndex:-1,a=0;return dg()&&(a|=128),{type:e,index:i,insertBeforeIndex:null,injectorIndex:s,directiveStart:-1,directiveEnd:-1,directiveStylingLast:-1,componentOffset:-1,controlDirectiveIndex:-1,customControlIndex:-1,propertyBindings:null,flags:a,providerIndexes:0,value:r,namespace:bg(),attrs:o,mergedAttrs:null,localNames:null,initialInputs:null,inputs:null,hostDirectiveInputs:null,outputs:null,hostDirectiveOutputs:null,directiveToIndex:null,tView:null,next:null,prev:null,projectionNext:null,child:null,parent:n,projection:null,styles:null,stylesWithoutHost:null,residualStyles:void 0,classes:null,classesWithoutHost:null,residualClasses:void 0,classBindings:0,styleBindings:0}}function kR(t){let n=t[Jp]??[],i=t[Lt][Ue],r=[];for(let o of n)o.data[fC]!==void 0?r.push(o):MR(o,i);t[Jp]=r}function MR(t,n){let e=0,i=t.firstChild;if(i){let r=t.data[mC];for(;e<r;){let o=i.nextSibling;SC(n,i,!1),i=o,e++}}}var TR=()=>null,AR=()=>null;function $u(t,n){return TR(t,n)}function aw(t,n,e){return AR(t,n,e)}var cw=class{},mm=class{},e_=class{resolveComponentFactory(n){throw new R(917,!1)}},Xc=class{static NULL=new e_},Ot=class{},xe=(()=>{class t{destroyNode=null;static __NG_ELEMENT_ID__=()=>RR()}return t})();function RR(){let t=ne(),n=jt(),e=Zn(n.index,t);return(ur(e)?e:t)[Ue]}var lw=(()=>{class t{static \u0275prov=C({token:t,providedIn:"root",factory:()=>null})}return t})();var Fu={},t_=class{injector;parentInjector;constructor(n,e){this.injector=n,this.parentInjector=e}get(n,e,i){let r=this.injector.get(n,Fu,i);return r!==Fu||e===Fu?r:this.parentInjector.get(n,e,i)}};function Gu(t,n,e){let i=e?t.styles:null,r=e?t.classes:null,o=0;if(n!==null)for(let s=0;s<n.length;s++){let a=n[s];if(typeof a=="number")o=a;else if(o==1)r=cu(r,a);else if(o==2){let c=a,l=n[++s];i=cu(i,c+": "+l+";")}}e?t.styles=i:t.stylesWithoutHost=i,e?t.classes=r:t.classesWithoutHost=r}function ee(t,n=0){let e=ne();if(e===null)return J(t,n);let i=jt();return iC(i,e,Qt(t),n)}function fm(){let t="invalid";throw new Error(t)}function dw(t,n,e,i,r){let o=i===null?null:{"":-1},s=r(t,e);if(s!==null){let a=s,c=null,l=null;for(let u of s)if(u.resolveHostDirectives!==null){[a,c,l]=u.resolveHostDirectives(s);break}PR(t,n,e,a,o,c,l)}o!==null&&i!==null&&OR(e,i,o)}function OR(t,n,e){let i=t.localNames=[];for(let r=0;r<n.length;r+=2){let o=e[n[r+1]];if(o==null)throw new R(-301,!1);i.push(n[r],o)}}function NR(t,n,e){n.componentOffset=e,(t.components??=[]).push(n.index)}function PR(t,n,e,i,r,o,s){let a=i.length,c=null;for(let g=0;g<a;g++){let v=i[g];c===null&&Li(v)&&(c=v,NR(t,e,g)),jg(Uu(e,n),t,v.type)}HR(e,t.data.length,a),c?.viewProvidersResolver&&c.viewProvidersResolver(c);for(let g=0;g<a;g++){let v=i[g];v.providersResolver&&v.providersResolver(v)}let l=!1,u=!1,m=NC(t,n,a,null);a>0&&(e.directiveToIndex=new Map);for(let g=0;g<a;g++){let v=i[g];if(e.mergedAttrs=Ys(e.mergedAttrs,v.hostAttrs),LR(t,e,n,m,v),VR(m,v,r),s!==null&&s.has(v)){let[F,j]=s.get(v);e.directiveToIndex.set(v.type,[m,F+e.directiveStart,j+e.directiveStart])}else(o===null||!o.has(v))&&e.directiveToIndex.set(v.type,m);v.contentQueries!==null&&(e.flags|=4),(v.hostBindings!==null||v.hostAttrs!==null||v.hostVars!==0)&&(e.flags|=64);let x=v.type.prototype;!l&&(x.ngOnChanges||x.ngOnInit||x.ngDoCheck)&&((t.preOrderHooks??=[]).push(e.index),l=!0),!u&&(x.ngOnChanges||x.ngDoCheck)&&((t.preOrderCheckHooks??=[]).push(e.index),u=!0),m++}FR(t,e,o)}function FR(t,n,e){for(let i=n.directiveStart;i<n.directiveEnd;i++){let r=t.data[i];if(e===null||!e.has(r))kx(0,n,r,i),kx(1,n,r,i),Tx(n,i,!1);else{let o=e.get(r);Mx(0,n,o,i),Mx(1,n,o,i),Tx(n,i,!0)}}}function kx(t,n,e,i){let r=t===0?e.inputs:e.outputs;for(let o in r)if(r.hasOwnProperty(o)){let s;t===0?s=n.inputs??={}:s=n.outputs??={},s[o]??=[],s[o].push(i),uw(n,o)}}function Mx(t,n,e,i){let r=t===0?e.inputs:e.outputs;for(let o in r)if(r.hasOwnProperty(o)){let s=r[o],a;t===0?a=n.hostDirectiveInputs??={}:a=n.hostDirectiveOutputs??={},a[s]??=[],a[s].push(i,o),uw(n,s)}}function uw(t,n){n==="class"?t.flags|=8:n==="style"&&(t.flags|=16)}function Tx(t,n,e){let{attrs:i,inputs:r,hostDirectiveInputs:o}=t;if(i===null||!e&&r===null||e&&o===null||T_(t)){t.initialInputs??=[],t.initialInputs.push(null);return}let s=null,a=0;for(;a<i.length;){let c=i[a];if(c===0){a+=4;continue}else if(c===5){a+=2;continue}else if(typeof c=="number")break;if(!e&&r.hasOwnProperty(c)){let l=r[c];for(let u of l)if(u===n){s??=[],s.push(c,i[a+1]);break}}else if(e&&o.hasOwnProperty(c)){let l=o[c];for(let u=0;u<l.length;u+=2)if(l[u]===n){s??=[],s.push(l[u+1],i[a+1]);break}}a+=2}t.initialInputs??=[],t.initialInputs.push(s)}function LR(t,n,e,i,r){t.data[i]=r;let o=r.factory||(r.factory=jr(r.type,!0)),s=new Qo(o,Li(r),ee,null);t.blueprint[i]=s,e[i]=s,jR(t,n,i,NC(t,e,r.hostVars,_n),r)}function jR(t,n,e,i,r){let o=r.hostBindings;if(o){let s=t.hostBindingOpCodes;s===null&&(s=t.hostBindingOpCodes=[]);let a=~n.index;BR(s)!=a&&s.push(a),s.push(e,i,o)}}function BR(t){let n=t.length;for(;n>0;){let e=t[--n];if(typeof e=="number"&&e<0)return e}return 0}function VR(t,n,e){if(e){if(n.exportAs)for(let i=0;i<n.exportAs.length;i++)e[n.exportAs[i]]=t;Li(n)&&(e[""]=t)}}function HR(t,n,e){t.flags|=1,t.directiveStart=n,t.directiveEnd=n+e,t.providerIndexes=n}function G_(t,n,e,i,r,o,s,a){let c=n[ie],l=c.consts,u=Pn(l,s),m=ea(c,t,e,i,u);return o&&dw(c,n,m,Pn(l,a),r),m.mergedAttrs=Ys(m.mergedAttrs,m.attrs),m.attrs!==null&&Gu(m,m.attrs,!1),m.mergedAttrs!==null&&Gu(m,m.mergedAttrs,!0),c.queries!==null&&c.queries.elementStart(c,m),m}function W_(t,n){Yx(t,n),eg(n)&&t.queries.elementEnd(n)}function UR(t,n,e,i,r,o){let s=n.consts,a=Pn(s,r),c=ea(n,t,e,i,a);if(c.mergedAttrs=Ys(c.mergedAttrs,c.attrs),o!=null){let l=Pn(s,o);c.localNames=[];for(let u=0;u<l.length;u+=2)c.localNames.push(l[u],-1)}return c.attrs!==null&&Gu(c,c.attrs,!1),c.mergedAttrs!==null&&Gu(c,c.mergedAttrs,!0),n.queries!==null&&n.queries.elementStart(n,c),c}function q_(t,n,e){return t[n]=e}function gn(t,n,e){if(e===_n)return!1;let i=t[n];return Object.is(i,e)?!1:(t[n]=e,!0)}function Y_(t,n,e,i){let r=gn(t,n,e);return gn(t,n+1,i)||r}function zR(t,n,e,i,r){let o=Y_(t,n,e,i);return gn(t,n+2,r)||o}function Lu(t,n,e){return function i(r){let o=i.__ngNativeEl__;o!==void 0&&BT(r,o);let s=Fi(t)?Zn(t.index,n):n;$_(s,5);let a=n[kt],c=Ax(n,a,e,r),l=i.__ngNextListenerFn__;for(;l;)c=Ax(n,a,l,r)&&c,l=l.__ngNextListenerFn__;return c}}function Ax(t,n,e,i){let r=te(null);try{return Be(Me.OutputStart,n,e),e(i)!==!1}catch(o){return aR(t,o),!1}finally{Be(Me.OutputEnd,n,e),te(r)}}function mw(t,n,e,i,r,o,s,a){let c=Hs(t),l=!1,u=null;if(!i&&c&&(u=GR(n,e,o,t.index)),u!==null){let m=u.__ngLastListenerFn__||u;m.__ngNextListenerFn__=s,u.__ngLastListenerFn__=s,l=!0}else{let m=Yn(t,e),g=i?i(m):m;HT(e,g,o,a),i||(a.__ngNativeEl__=m);let v=r.listen(g,o,a);if(!$R(o)){let x=i?F=>i(qn(F[t.index])):t.index;fw(x,n,e,o,a,v,!1)}}return l}function $R(t){return t.startsWith("animation")||t.startsWith("transition")}function GR(t,n,e,i){let r=t.cleanup;if(r!=null)for(let o=0;o<r.length-1;o+=2){let s=r[o];if(s===e&&r[o+1]===i){let a=n[Bs],c=r[o+2];return a&&a.length>c?a[c]:null}typeof s=="string"&&(o+=2)}return null}function fw(t,n,e,i,r,o,s){let a=n.firstCreatePass?cg(n):null,c=ag(e),l=c.length;c.push(r,o),a&&a.push(i,t,l,(l+1)*(s?-1:1))}function Rx(t,n,e,i,r,o){let s=n[e],a=n[ie],l=a.data[e].outputs[i],m=s[l].subscribe(o);fw(t.index,a,n,r,o,m,!0)}var n_=Symbol("BINDING");function hw(t){return t.debugInfo?.className||t.type.name||null}var Wu=class extends Xc{ngModule;constructor(n){super(),this.ngModule=n}resolveComponentFactory(n){let e=cr(n);return new Ko(e,this.ngModule)}};function WR(t){return Object.keys(t).map(n=>{let[e,i,r]=t[n],o={propName:e,templateName:n,isSignal:(i&rm.SignalBased)!==0};return r&&(o.transform=r),o})}function qR(t){return Object.keys(t).map(n=>({propName:t[n],templateName:n}))}function YR(t,n,e){let i=n instanceof He?n:n?.injector;return i&&t.getStandaloneInjector!==null&&(i=t.getStandaloneInjector(i)||i),i?new t_(e,i):e}function ZR(t){let n=t.get(Ot,null);if(n===null)throw new R(407,!1);let e=t.get(lw,null),i=t.get(Ri,null),r=t.get(_i,null,{optional:!0});return{rendererFactory:n,sanitizer:e,changeDetectionScheduler:i,ngReflect:!1,tracingService:r}}function QR(t,n){let e=pw(t);return DC(n,e,e==="svg"?tg:e==="math"?F0:null)}function pw(t){return(t.selectors[0][0]||"div").toLowerCase()}var Ko=class extends mm{componentDef;ngModule;selector;componentType;ngContentSelectors;isBoundToModule;cachedInputs=null;cachedOutputs=null;get inputs(){return this.cachedInputs??=WR(this.componentDef.inputs),this.cachedInputs}get outputs(){return this.cachedOutputs??=qR(this.componentDef.outputs),this.cachedOutputs}constructor(n,e){super(),this.componentDef=n,this.ngModule=e,this.componentType=n.type,this.selector=kA(n.selectors),this.ngContentSelectors=n.ngContentSelectors??[],this.isBoundToModule=!!e}create(n,e,i,r,o,s){Be(Me.DynamicComponentStart);let a=te(null);try{let c=this.componentDef,l=YR(c,r||this.ngModule,n),u=ZR(l),m=u.tracingService;return m&&m.componentCreate?m.componentCreate(hw(c),()=>this.createComponentRef(u,l,e,i,o,s)):this.createComponentRef(u,l,e,i,o,s)}finally{te(a)}}createComponentRef(n,e,i,r,o,s){let a=this.componentDef,c=XR(r,a,s,o),l=n.rendererFactory.createRenderer(null,a),u=r?KA(l,r,a.encapsulation,e):QR(a,l),m=s?.some(Ox)||o?.some(x=>typeof x!="function"&&x.bindings.some(Ox)),g=R_(null,c,null,512|OC(a),null,null,n,l,e,null,gC(u,e,!0));g[at]=u,Du(g);let v=null;try{let x=G_(at,g,2,"#host",()=>c.directiveRegistry,!0,0);IC(l,u,x),Zs(u,g),lm(c,g,x),C_(c,x,g),W_(c,x),i!==void 0&&JR(x,this.ngContentSelectors,i),v=Zn(x.index,g),g[kt]=v[kt],z_(c,g,null)}catch(x){throw v!==null&&Vg(v),Vg(g),x}finally{Be(Me.DynamicComponentEnd),Eu()}return new qu(this.componentType,g,!!m)}};function XR(t,n,e,i){let r=t?["ng-version","21.2.17"]:MA(n.selectors[0]),o=null,s=null,a=0;if(e)for(let u of e)a+=u[n_].requiredVars,u.create&&(u.targetIdx=0,(o??=[]).push(u)),u.update&&(u.targetIdx=0,(s??=[]).push(u));if(i)for(let u=0;u<i.length;u++){let m=i[u];if(typeof m!="function")for(let g of m.bindings){a+=g[n_].requiredVars;let v=u+1;g.create&&(g.targetIdx=v,(o??=[]).push(g)),g.update&&(g.targetIdx=v,(s??=[]).push(g))}}let c=[n];if(i)for(let u of i){let m=typeof u=="function"?u:u.type,g=Up(m);c.push(g)}return A_(0,null,KR(o,s),1,a,c,null,null,null,[r],null)}function KR(t,n){return!t&&!n?null:e=>{if(e&1&&t)for(let i of t)i.create();if(e&2&&n)for(let i of n)i.update()}}function Ox(t){let n=t[n_].kind;return n==="input"||n==="twoWay"}var qu=class extends cw{_rootLView;_hasInputBindings;instance;hostView;changeDetectorRef;componentType;location;previousInputValues=null;_tNode;constructor(n,e,i){super(),this._rootLView=e,this._hasInputBindings=i,this._tNode=gu(e[ie],at),this.location=Js(this._tNode,e),this.instance=Zn(this._tNode.index,e)[kt],this.hostView=this.changeDetectorRef=new qr(e,void 0),this.componentType=n}setInput(n,e){this._hasInputBindings;let i=this._tNode;if(this.previousInputValues??=new Map,this.previousInputValues.has(n)&&Object.is(this.previousInputValues.get(n),e))return;let r=this._rootLView,o=dm(i,r[ie],r,n,e);this.previousInputValues.set(n,e);let s=Zn(i.index,r);$_(s,1)}get injector(){return new Zo(this._tNode,this._rootLView)}destroy(){this.hostView.destroy()}onDestroy(n){this.hostView.onDestroy(n)}};function JR(t,n,e){let i=t.projection=[];for(let r=0;r<n.length;r++){let o=e[r];i.push(o!=null&&o.length?Array.from(o):null)}}var Vt=(()=>{class t{static __NG_ELEMENT_ID__=e1}return t})();function e1(){let t=jt();return gw(t,ne())}var i_=class t extends Vt{_lContainer;_hostTNode;_hostLView;constructor(n,e,i){super(),this._lContainer=n,this._hostTNode=e,this._hostLView=i}get element(){return Js(this._hostTNode,this._hostLView)}get injector(){return new Zo(this._hostTNode,this._hostLView)}get parentInjector(){let n=b_(this._hostTNode,this._hostLView);if(Xx(n)){let e=Vu(n,this._hostLView),i=Bu(n),r=e[ie].data[i+8];return new Zo(r,e)}else return new Zo(null,this._hostLView)}clear(){for(;this.length>0;)this.remove(this.length-1)}get(n){let e=Nx(this._lContainer);return e!==null&&e[n]||null}get length(){return this._lContainer.length-_t}createEmbeddedView(n,e,i){let r,o;typeof i=="number"?r=i:i!=null&&(r=i.index,o=i.injector);let s=$u(this._lContainer,n.ssrId),a=n.createEmbeddedViewImpl(e||{},o,s);return this.insertImpl(a,r,Qs(this._hostTNode,s)),a}createComponent(n,e,i,r,o,s,a){let c=n&&!fT(n),l;if(c)l=e;else{let j=e||{};l=j.index,i=j.injector,r=j.projectableNodes,o=j.environmentInjector||j.ngModuleRef,s=j.directives,a=j.bindings}let u=c?n:new Ko(cr(n)),m=i||this.parentInjector;if(!o&&u.ngModule==null){let Y=(c?m:this.parentInjector).get(He,null);Y&&(o=Y)}let g=cr(u.componentType??{}),v=$u(this._lContainer,g?.id??null),x=v?.firstChild??null,F=u.create(m,r,x,o,s,a);return this.insertImpl(F.hostView,l,Qs(this._hostTNode,v)),F}insert(n,e){return this.insertImpl(n,e,!0)}insertImpl(n,e,i){let r=n._lView;if(j0(r)){let a=this.indexOf(n);if(a!==-1)this.detach(a);else{let c=r[Lt],l=new t(c,c[hn],c[Lt]);l.detach(l.indexOf(n))}}let o=this._adjustIndex(e),s=this._lContainer;return Qc(s,r,o,i),n.attachToViewContainerRef(),Gp(Mg(s),o,n),n}move(n,e){return this.insert(n,e)}indexOf(n){let e=Nx(this._lContainer);return e!==null?e.indexOf(n):-1}remove(n){let e=this._adjustIndex(n,-1),i=Vc(this._lContainer,e);i&&(Dc(Mg(this._lContainer),e),am(i[ie],i))}detach(n){let e=this._adjustIndex(n,-1),i=Vc(this._lContainer,e);return i&&Dc(Mg(this._lContainer),e)!=null?new qr(i):null}_adjustIndex(n,e=0){return n??this.length+e}};function Nx(t){return t[Sc]}function Mg(t){return t[Sc]||(t[Sc]=[])}function gw(t,n){let e,i=n[t.index];return mi(i)?e=i:(e=iw(i,n,null,t),n[t.index]=e,O_(n,e)),n1(e,n,t,i),new i_(e,t,n)}function t1(t,n){let e=t[Ue],i=e.createComment(""),r=Yn(n,t),o=e.parentNode(r);return zu(e,o,i,e.nextSibling(r),!1),i}var n1=o1,i1=()=>!1;function r1(t,n,e){return i1(t,n,e)}function o1(t,n,e,i){if(t[Gr])return;let r;e.type&8?r=qn(i):r=t1(n,e),t[Gr]=r}var r_=class t{queryList;matches=null;constructor(n){this.queryList=n}clone(){return new t(this.queryList)}setDirty(){this.queryList.setDirty()}},o_=class t{queries;constructor(n=[]){this.queries=n}createEmbeddedView(n){let e=n.queries;if(e!==null){let i=n.contentQueries!==null?n.contentQueries[0]:e.length,r=[];for(let o=0;o<i;o++){let s=e.getByIndex(o),a=this.queries[s.indexInDeclarationView];r.push(a.clone())}return new t(r)}return null}insertView(n){this.dirtyQueriesWithMatches(n)}detachView(n){this.dirtyQueriesWithMatches(n)}finishViewCreation(n){this.dirtyQueriesWithMatches(n)}dirtyQueriesWithMatches(n){for(let e=0;e<this.queries.length;e++)Q_(n,e).matches!==null&&this.queries[e].setDirty()}},Yu=class{flags;read;predicate;constructor(n,e,i=null){this.flags=e,this.read=i,typeof n=="string"?this.predicate=d1(n):this.predicate=n}},s_=class t{queries;constructor(n=[]){this.queries=n}elementStart(n,e){for(let i=0;i<this.queries.length;i++)this.queries[i].elementStart(n,e)}elementEnd(n){for(let e=0;e<this.queries.length;e++)this.queries[e].elementEnd(n)}embeddedTView(n){let e=null;for(let i=0;i<this.length;i++){let r=e!==null?e.length:0,o=this.getByIndex(i).embeddedTView(n,r);o&&(o.indexInDeclarationView=i,e!==null?e.push(o):e=[o])}return e!==null?new t(e):null}template(n,e){for(let i=0;i<this.queries.length;i++)this.queries[i].template(n,e)}getByIndex(n){return this.queries[n]}get length(){return this.queries.length}track(n){this.queries.push(n)}},a_=class t{metadata;matches=null;indexInDeclarationView=-1;crossesNgTemplate=!1;_declarationNodeIndex;_appliesToNextNode=!0;constructor(n,e=-1){this.metadata=n,this._declarationNodeIndex=e}elementStart(n,e){this.isApplyingToNode(e)&&this.matchTNode(n,e)}elementEnd(n){this._declarationNodeIndex===n.index&&(this._appliesToNextNode=!1)}template(n,e){this.elementStart(n,e)}embeddedTView(n,e){return this.isApplyingToNode(n)?(this.crossesNgTemplate=!0,this.addMatch(-n.index,e),new t(this.metadata)):null}isApplyingToNode(n){if(this._appliesToNextNode&&(this.metadata.flags&1)!==1){let e=this._declarationNodeIndex,i=n.parent;for(;i!==null&&i.type&8&&i.index!==e;)i=i.parent;return e===(i!==null?i.index:-1)}return this._appliesToNextNode}matchTNode(n,e){let i=this.metadata.predicate;if(Array.isArray(i))for(let r=0;r<i.length;r++){let o=i[r];this.matchTNodeWithReadOption(n,e,s1(e,o)),this.matchTNodeWithReadOption(n,e,Pu(e,n,o,!1,!1))}else i===vt?e.type&4&&this.matchTNodeWithReadOption(n,e,-1):this.matchTNodeWithReadOption(n,e,Pu(e,n,i,!1,!1))}matchTNodeWithReadOption(n,e,i){if(i!==null){let r=this.metadata.read;if(r!==null)if(r===L||r===Vt||r===vt&&e.type&4)this.addMatch(e.index,-2);else{let o=Pu(e,n,r,!1,!1);o!==null&&this.addMatch(e.index,o)}else this.addMatch(e.index,i)}}addMatch(n,e){this.matches===null?this.matches=[n,e]:this.matches.push(n,e)}};function s1(t,n){let e=t.localNames;if(e!==null){for(let i=0;i<e.length;i+=2)if(e[i]===n)return e[i+1]}return null}function a1(t,n){return t.type&11?Js(t,n):t.type&4?um(t,n):null}function c1(t,n,e,i){return e===-1?a1(n,t):e===-2?l1(t,n,i):Lc(t,t[ie],e,n)}function l1(t,n,e){if(e===L)return Js(n,t);if(e===vt)return um(n,t);if(e===Vt)return gw(n,t)}function _w(t,n,e,i){let r=n[Pi].queries[i];if(r.matches===null){let o=t.data,s=e.matches,a=[];for(let c=0;s!==null&&c<s.length;c+=2){let l=s[c];if(l<0)a.push(null);else{let u=o[l];a.push(c1(n,u,s[c+1],e.metadata.read))}}r.matches=a}return r.matches}function c_(t,n,e,i){let r=t.queries.getByIndex(e),o=r.matches;if(o!==null){let s=_w(t,n,r,e);for(let a=0;a<o.length;a+=2){let c=o[a];if(c>0)i.push(s[a/2]);else{let l=o[a+1],u=n[-c];for(let m=_t;m<u.length;m++){let g=u[m];g[zr]===g[Lt]&&c_(g[ie],g,l,i)}if(u[Wo]!==null){let m=u[Wo];for(let g=0;g<m.length;g++){let v=m[g];c_(v[ie],v,l,i)}}}}}return i}function Z_(t,n){return t[Pi].queries[n].queryList}function vw(t,n,e){let i=new En((e&4)===4);return H0(t,n,i,i.destroy),(n[Pi]??=new o_).queries.push(new r_(i))-1}function bw(t,n,e){let i=Qe();return i.firstCreatePass&&(xw(i,new Yu(t,n,e),-1),(n&2)===2&&(i.staticViewQueries=!0)),vw(i,ne(),n)}function yw(t,n,e,i){let r=Qe();if(r.firstCreatePass){let o=jt();xw(r,new Yu(n,e,i),o.index),u1(r,t),(e&2)===2&&(r.staticContentQueries=!0)}return vw(r,ne(),e)}function d1(t){return t.split(",").map(n=>n.trim())}function xw(t,n,e){t.queries===null&&(t.queries=new s_),t.queries.track(new a_(n,e))}function u1(t,n){let e=t.contentQueries||(t.contentQueries=[]),i=e.length?e[e.length-1]:-1;n!==i&&e.push(t.queries.length-1,n)}function Q_(t,n){return t.queries.getByIndex(n)}function Cw(t,n){let e=t[ie],i=Q_(e,n);return i.crossesNgTemplate?c_(e,t,n,[]):_w(e,t,i,n)}function ww(t,n,e){let i,r=ic(()=>{i._dirtyCounter();let o=m1(i,t);if(n&&o===void 0)throw new R(-951,!1);return o});return i=r[Et],i._dirtyCounter=M(0),i._flatValue=void 0,r}function X_(t){return ww(!0,!1,t)}function K_(t){return ww(!0,!0,t)}function Dw(t,n){let e=t[Et];e._lView=ne(),e._queryIndex=n,e._queryList=Z_(e._lView,n),e._queryList.onDirty(()=>e._dirtyCounter.update(i=>i+1))}function m1(t,n){let e=t._lView,i=t._queryIndex;if(e===void 0||i===void 0||e[se]&4)return n?void 0:nn;let r=Z_(e,i),o=Cw(e,i);return r.reset(o,sC),n?r.first:r._changesDetected||t._flatValue===void 0?t._flatValue=r.toArray():t._flatValue}var zi=class{},hm=class{};var Zu=class extends zi{ngModuleType;_parent;_bootstrapComponents=[];_r3Injector;instance;destroyCbs=[];componentFactoryResolver=new Wu(this);constructor(n,e,i,r=!0){super(),this.ngModuleType=n,this._parent=e;let o=Hp(n);this._bootstrapComponents=TC(o.bootstrap),this._r3Injector=yg(n,e,[{provide:zi,useValue:this},{provide:Xc,useValue:this.componentFactoryResolver},...i],yc(n),new Set(["environment"])),r&&this.resolveInjectorInitializers()}resolveInjectorInitializers(){this._r3Injector.resolveInjectorInitializers(),this.instance=this._r3Injector.get(this.ngModuleType)}get injector(){return this._r3Injector}destroy(){let n=this._r3Injector;!n.destroyed&&n.destroy(),this.destroyCbs.forEach(e=>e()),this.destroyCbs=null}onDestroy(n){this.destroyCbs.push(n)}},Qu=class extends hm{moduleType;constructor(n){super(),this.moduleType=n}create(n){return new Zu(this.moduleType,n,[])}};var Hc=class extends zi{injector;componentFactoryResolver=new Wu(this);instance=null;constructor(n){super();let e=new Bo([...n.providers,{provide:zi,useValue:this},{provide:Xc,useValue:this.componentFactoryResolver}],n.parent||js(),n.debugName,new Set(["environment"]));this.injector=e,n.runEnvironmentInitializers&&e.resolveInjectorInitializers()}destroy(){this.injector.destroy()}onDestroy(n){this.injector.onDestroy(n)}};function Kc(t,n,e=null){return new Hc({providers:t,parent:n,debugName:e,runEnvironmentInitializers:!0}).injector}var f1=(()=>{class t{_injector;cachedInjectors=new Map;constructor(e){this._injector=e}getOrCreateStandaloneInjector(e){if(!e.standalone)return null;if(!this.cachedInjectors.has(e)){let i=Yp(!1,e.type),r=i.length>0?Kc([i],this._injector,""):null;this.cachedInjectors.set(e,r)}return this.cachedInjectors.get(e)}ngOnDestroy(){try{for(let e of this.cachedInjectors.values())e!==null&&e.destroy()}finally{this.cachedInjectors.clear()}}static \u0275prov=C({token:t,providedIn:"environment",factory:()=>new t(J(He))})}return t})();function E(t){return zc(()=>{let n=Ew(t),e=Z(b({},n),{decls:t.decls,vars:t.vars,template:t.template,consts:t.consts||null,ngContentSelectors:t.ngContentSelectors,onPush:t.changeDetection===y_.OnPush,directiveDefs:null,pipeDefs:null,dependencies:n.standalone&&t.dependencies||null,getStandaloneInjector:n.standalone?r=>r.get(f1).getOrCreateStandaloneInjector(e):null,getExternalStyles:null,signals:t.signals??!1,data:t.data||{},encapsulation:t.encapsulation||pi.Emulated,styles:t.styles||nn,_:null,schemas:t.schemas||null,tView:null,id:""});n.standalone&&Zr("NgStandalone"),Sw(e);let i=t.dependencies;return e.directiveDefs=Px(i,h1),e.pipeDefs=Px(i,x0),e.id=_1(e),e})}function h1(t){return cr(t)||Up(t)}function ae(t){return zc(()=>({type:t.type,bootstrap:t.bootstrap||nn,declarations:t.declarations||nn,imports:t.imports||nn,exports:t.exports||nn,transitiveCompileScopes:null,schemas:t.schemas||null,id:t.id||null}))}function p1(t,n){if(t==null)return Hr;let e={};for(let i in t)if(t.hasOwnProperty(i)){let r=t[i],o,s,a,c;Array.isArray(r)?(a=r[0],o=r[1],s=r[2]??o,c=r[3]||null):(o=r,s=r,a=rm.None,c=null),e[o]=[i,a,c],n[o]=s}return e}function g1(t){if(t==null)return Hr;let n={};for(let e in t)t.hasOwnProperty(e)&&(n[t[e]]=e);return n}function A(t){return zc(()=>{let n=Ew(t);return Sw(n),n})}function Qr(t){return{type:t.type,name:t.name,factory:null,pure:t.pure!==!1,standalone:t.standalone??!0,onDestroy:t.type.prototype.ngOnDestroy||null}}function Ew(t){let n={};return{type:t.type,providersResolver:null,viewProvidersResolver:null,factory:null,hostBindings:t.hostBindings||null,hostVars:t.hostVars||0,hostAttrs:t.hostAttrs||null,contentQueries:t.contentQueries||null,declaredInputs:n,inputConfig:t.inputs||Hr,exportAs:t.exportAs||null,standalone:t.standalone??!0,signals:t.signals===!0,selectors:t.selectors||nn,viewQuery:t.viewQuery||null,features:t.features||null,setInput:null,resolveHostDirectives:null,hostDirectives:null,controlDef:null,inputs:p1(t.inputs,n),outputs:g1(t.outputs),debugInfo:null}}function Sw(t){t.features?.forEach(n=>n(t))}function Px(t,n){return t?()=>{let e=typeof t=="function"?t():t,i=[];for(let r of e){let o=n(r);o!==null&&i.push(o)}return i}:null}function _1(t){let n=0,e=typeof t.consts=="function"?"":t.consts,i=[t.selectors,t.ngContentSelectors,t.hostVars,t.hostAttrs,e,t.vars,t.decls,t.encapsulation,t.standalone,t.signals,t.exportAs,JSON.stringify(t.inputs),JSON.stringify(t.outputs),Object.getOwnPropertyNames(t.type.prototype),!!t.contentQueries,!!t.viewQuery];for(let o of i.join("|"))n=Math.imul(31,n)+o.charCodeAt(0)<<0;return n+=2147483648,"c"+n}function v1(t){return Object.getPrototypeOf(t.prototype).constructor}function fe(t){let n=v1(t.type),e=!0,i=[t];for(;n&&n!==Function.prototype&&n!==Object.prototype;){let r,o=Object.hasOwn(n,Cc)?n[Cc]:void 0,s=Object.hasOwn(n,wc)?n[wc]:void 0;if(Li(t))r=o??s;else{if(o)throw new R(903,!1);r=s}if(r){if(e){i.push(r);let c=t;c.inputs=Tg(t.inputs),c.declaredInputs=Tg(t.declaredInputs),c.outputs=Tg(t.outputs);let l=r.hostBindings;l&&w1(t,l);let u=r.viewQuery,m=r.contentQueries;if(u&&x1(t,u),m&&C1(t,m),b1(t,r),y0(t.outputs,r.outputs),Li(r)&&r.data.animation){let g=t.data;g.animation=(g.animation||[]).concat(r.data.animation)}}let a=r.features;if(a)for(let c=0;c<a.length;c++){let l=a[c];l&&l.ngInherit&&l(t),l===fe&&(e=!1)}}n=Object.getPrototypeOf(n)}y1(i)}function b1(t,n){for(let e in n.inputs){if(!n.inputs.hasOwnProperty(e)||t.inputs.hasOwnProperty(e))continue;let i=n.inputs[e];i!==void 0&&(t.inputs[e]=i,t.declaredInputs[e]=n.declaredInputs[e])}}function y1(t){let n=0,e=null;for(let i=t.length-1;i>=0;i--){let r=t[i];r.hostVars=n+=r.hostVars,r.hostAttrs=Ys(r.hostAttrs,e=Ys(e,r.hostAttrs))}}function Tg(t){return t===Hr?{}:t===nn?[]:t}function x1(t,n){let e=t.viewQuery;e?t.viewQuery=(i,r)=>{n(i,r),e(i,r)}:t.viewQuery=n}function C1(t,n){let e=t.contentQueries;e?t.contentQueries=(i,r,o)=>{n(i,r,o),e(i,r,o)}:t.contentQueries=n}function w1(t,n){let e=t.hostBindings;e?t.hostBindings=(i,r)=>{n(i,r),e(i,r)}:t.hostBindings=n}function Iw(t,n,e,i,r,o,s,a){if(e.firstCreatePass){t.mergedAttrs=Ys(t.mergedAttrs,t.attrs);let u=t.tView=A_(2,t,r,o,s,e.directiveRegistry,e.pipeRegistry,null,e.schemas,e.consts,null);e.queries!==null&&(e.queries.template(e,t),u.queries=e.queries.embeddedTView(t))}a&&(t.flags|=a),zs(t,!1);let c=E1(e,n,t,i);Su()&&j_(e,n,c,t),Zs(c,n);let l=iw(c,n,c,t);n[i+at]=l,O_(n,l),r1(l,t,n)}function D1(t,n,e,i,r,o,s,a,c,l,u){let m=e+at,g;return n.firstCreatePass?(g=ea(n,m,4,s||null,a||null),yu()&&dw(n,t,g,Pn(n.consts,l),V_),Yx(n,g)):g=n.data[m],Iw(g,t,n,e,i,r,o,c),Hs(g)&&lm(n,t,g),l!=null&&Yc(t,g,u),g}function Xs(t,n,e,i,r,o,s,a,c,l,u){let m=e+at,g;if(n.firstCreatePass){if(g=ea(n,m,4,s||null,a||null),l!=null){let v=Pn(n.consts,l);g.localNames=[];for(let x=0;x<v.length;x+=2)g.localNames.push(v[x],-1)}}else g=n.data[m];return Iw(g,t,n,e,i,r,o,c),l!=null&&Yc(t,g,u),g}function $e(t,n,e,i,r,o,s,a){let c=ne(),l=Qe(),u=Pn(l.consts,o);return D1(c,l,t,n,e,i,r,u,void 0,s,a),$e}function ta(t,n,e,i,r,o,s,a){let c=ne(),l=Qe(),u=Pn(l.consts,o);return Xs(c,l,t,n,e,i,r,u,void 0,s,a),ta}var E1=S1;function S1(t,n,e,i){return Tc(!0),n[Ue].createComment("")}var pm=(()=>{class t{log(e){console.log(e)}warn(e){console.warn(e)}static \u0275fac=function(i){return new(i||t)};static \u0275prov=C({token:t,factory:t.\u0275fac,providedIn:"platform"})}return t})();function $i(t){return typeof t=="function"&&t[Et]!==void 0}function J_(t){return $i(t)&&typeof t.set=="function"}var ev=new y("");function Xr(t){return!!t&&typeof t.then=="function"}function tv(t){return!!t&&typeof t.subscribe=="function"}var nv=new y("");function gm(t){return Ni([{provide:nv,multi:!0,useValue:t}])}var iv=(()=>{class t{resolve;reject;initialized=!1;done=!1;donePromise=new Promise((e,i)=>{this.resolve=e,this.reject=i});appInits=d(nv,{optional:!0})??[];injector=d(X);constructor(){}runInitializers(){if(this.initialized)return;let e=[];for(let r of this.appInits){let o=$t(this.injector,r);if(Xr(o))e.push(o);else if(tv(o)){let s=new Promise((a,c)=>{o.subscribe({complete:a,error:c})});e.push(s)}}let i=()=>{this.done=!0,this.resolve()};Promise.all(e).then(()=>{i()}).catch(r=>{this.reject(r)}),e.length===0&&i(),this.initialized=!0}static \u0275fac=function(i){return new(i||t)};static \u0275prov=C({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})(),_m=new y("");function kw(){ap(()=>{let t="";throw new R(600,t)})}function Mw(t){return t.isBoundToModule}var I1=10;var Sn=(()=>{class t{_runningTick=!1;_destroyed=!1;_destroyListeners=[];_views=[];internalErrorHandler=d(Fn);afterRenderManager=d(sm);zonelessEnabled=d(Ac);rootEffectScheduler=d(ku);dirtyFlags=0;tracingSnapshot=null;allTestViews=new Set;autoDetectTestViews=new Set;includeAllTestViews=!1;afterTick=new I;get allViews(){return[...(this.includeAllTestViews?this.allTestViews:this.autoDetectTestViews).keys(),...this._views]}get destroyed(){return this._destroyed}componentTypes=[];components=[];internalPendingTask=d(mr);get isStable(){return this.internalPendingTask.hasPendingTasksObservable.pipe(Q(e=>!e))}constructor(){d(_i,{optional:!0})}whenStable(){let e;return new Promise(i=>{e=this.isStable.subscribe({next:r=>{r&&i()}})}).finally(()=>{e.unsubscribe()})}_injector=d(He);_rendererFactory=null;get injector(){return this._injector}bootstrap(e,i){return this.bootstrapImpl(e,i)}bootstrapImpl(e,i,r=X.NULL){return this._injector.get(H).run(()=>{Be(Me.BootstrapComponentStart);let s=e instanceof mm;if(!this._injector.get(iv).done){let x="";throw new R(405,x)}let c;s?c=e:c=this._injector.get(Xc).resolveComponentFactory(e),this.componentTypes.push(c.componentType);let l=Mw(c)?void 0:this._injector.get(zi),u=i||c.selector,m=c.create(r,[],u,l),g=m.location.nativeElement,v=m.injector.get(ev,null);return v?.registerApplication(g),m.onDestroy(()=>{this.detachView(m.hostView),Fc(this.components,m),v?.unregisterApplication(g)}),this._loadComponent(m),Be(Me.BootstrapComponentEnd,m),m})}tick(){this.zonelessEnabled||(this.dirtyFlags|=1),this._tick()}_tick(){Be(Me.ChangeDetectionStart),this.tracingSnapshot!==null?this.tracingSnapshot.run(om.CHANGE_DETECTION,this.tickImpl):this.tickImpl()}tickImpl=()=>{if(this._runningTick)throw Be(Me.ChangeDetectionEnd),new R(101,!1);let e=te(null);try{this._runningTick=!0,this.synchronize()}finally{this._runningTick=!1,this.tracingSnapshot?.dispose(),this.tracingSnapshot=null,te(e),this.afterTick.next(),Be(Me.ChangeDetectionEnd)}};synchronize(){this._rendererFactory===null&&!this._injector.destroyed&&(this._rendererFactory=this._injector.get(Ot,null,{optional:!0}));let e=0;for(;this.dirtyFlags!==0&&e++<I1;){Be(Me.ChangeDetectionSyncStart);try{this.synchronizeOnce()}finally{Be(Me.ChangeDetectionSyncEnd)}}}synchronizeOnce(){this.dirtyFlags&16&&(this.dirtyFlags&=-17,this.rootEffectScheduler.flush());let e=!1;if(this.dirtyFlags&7){let i=!!(this.dirtyFlags&1);this.dirtyFlags&=-8,this.dirtyFlags|=8;for(let{_lView:r}of this.allViews){if(!i&&!Ic(r))continue;let o=i&&!this.zonelessEnabled?0:1;JC(r,o),e=!0}if(this.dirtyFlags&=-5,this.syncDirtyFlagsWithViews(),this.dirtyFlags&23)return}e||(this._rendererFactory?.begin?.(),this._rendererFactory?.end?.()),this.dirtyFlags&8&&(this.dirtyFlags&=-9,this.afterRenderManager.execute()),this.syncDirtyFlagsWithViews()}syncDirtyFlagsWithViews(){if(this.allViews.some(({_lView:e})=>Ic(e))){this.dirtyFlags|=2;return}else this.dirtyFlags&=-8}attachView(e){let i=e;this._views.push(i),i.attachToAppRef(this)}detachView(e){let i=e;Fc(this._views,i),i.detachFromAppRef()}_loadComponent(e){this.attachView(e.hostView);try{this.tick()}catch(r){this.internalErrorHandler(r)}this.components.push(e),this._injector.get(_m,[]).forEach(r=>r(e))}ngOnDestroy(){if(!this._destroyed)try{this._destroyListeners.forEach(e=>e()),this._views.slice().forEach(e=>e.destroy())}finally{this._destroyed=!0,this._views=[],this._destroyListeners=[]}}onDestroy(e){return this._destroyListeners.push(e),()=>Fc(this._destroyListeners,e)}destroy(){if(this._destroyed)throw new R(406,!1);let e=this._injector;e.destroy&&!e.destroyed&&e.destroy()}get viewCount(){return this._views.length}static \u0275fac=function(i){return new(i||t)};static \u0275prov=C({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();function Fc(t,n){let e=t.indexOf(n);e>-1&&t.splice(e,1)}function vm(t,n){let e=ne(),i=ji();if(gn(e,i,n)){let r=Qe(),o=$s();if(dm(o,r,e,t,n))Fi(o)&&WC(e,o.index);else{let a=Yn(o,e);qC(e[Ue],a,null,o.value,t,n,null)}}return vm}function q(t,n,e,i){let r=ne(),o=ji();if(gn(r,o,n)){let s=Qe(),a=$s();oR(a,r,t,n,e,i)}return q}var l_=class{destroy(n){}updateValue(n,e){}swap(n,e){let i=Math.min(n,e),r=Math.max(n,e),o=this.detach(r);if(r-i>1){let s=this.detach(i);this.attach(i,o),this.attach(r,s)}else this.attach(i,o)}move(n,e){this.attach(e,this.detach(n))}};function Ag(t,n,e,i,r){return t===e&&Object.is(n,i)?1:Object.is(r(t,n),r(e,i))?-1:0}function k1(t,n,e,i){let r,o,s=0,a=t.length-1,c=void 0;if(Array.isArray(n)){te(i);let l=n.length-1;for(te(null);s<=a&&s<=l;){let u=t.at(s),m=n[s],g=Ag(s,u,s,m,e);if(g!==0){g<0&&t.updateValue(s,m),s++;continue}let v=t.at(a),x=n[l],F=Ag(a,v,l,x,e);if(F!==0){F<0&&t.updateValue(a,x),a--,l--;continue}let j=e(s,u),Y=e(a,v),be=e(s,m);if(Object.is(be,Y)){let Dt=e(l,x);Object.is(Dt,j)?(t.swap(s,a),t.updateValue(a,x),l--,a--):t.move(a,s),t.updateValue(s,m),s++;continue}if(r??=new Xu,o??=Lx(t,s,a,e),d_(t,r,s,be))t.updateValue(s,m),s++,a++;else if(o.has(be))r.set(j,t.detach(s)),a--;else{let Dt=t.create(s,n[s]);t.attach(s,Dt),s++,a++}}for(;s<=l;)Fx(t,r,e,s,n[s]),s++}else if(n!=null){te(i);let l=n[Symbol.iterator]();te(null);let u=l.next();for(;!u.done&&s<=a;){let m=t.at(s),g=u.value,v=Ag(s,m,s,g,e);if(v!==0)v<0&&t.updateValue(s,g),s++,u=l.next();else{r??=new Xu,o??=Lx(t,s,a,e);let x=e(s,g);if(d_(t,r,s,x))t.updateValue(s,g),s++,a++,u=l.next();else if(!o.has(x))t.attach(s,t.create(s,g)),s++,a++,u=l.next();else{let F=e(s,m);r.set(F,t.detach(s)),a--}}}for(;!u.done;)Fx(t,r,e,t.length,u.value),u=l.next()}for(;s<=a;)t.destroy(t.detach(a--));r?.forEach(l=>{t.destroy(l)})}function d_(t,n,e,i){return n!==void 0&&n.has(i)?(t.attach(e,n.get(i)),n.delete(i),!0):!1}function Fx(t,n,e,i,r){if(d_(t,n,i,e(i,r)))t.updateValue(i,r);else{let o=t.create(i,r);t.attach(i,o)}}function Lx(t,n,e,i){let r=new Set;for(let o=n;o<=e;o++)r.add(i(o,t.at(o)));return r}var Xu=class{kvMap=new Map;_vMap=void 0;has(n){return this.kvMap.has(n)}delete(n){if(!this.has(n))return!1;let e=this.kvMap.get(n);return this._vMap!==void 0&&this._vMap.has(e)?(this.kvMap.set(n,this._vMap.get(e)),this._vMap.delete(e)):this.kvMap.delete(n),!0}get(n){return this.kvMap.get(n)}set(n,e){if(this.kvMap.has(n)){let i=this.kvMap.get(n);this._vMap===void 0&&(this._vMap=new Map);let r=this._vMap;for(;r.has(i);)i=r.get(i);r.set(i,e)}else this.kvMap.set(n,e)}forEach(n){for(let[e,i]of this.kvMap)if(n(i,e),this._vMap!==void 0){let r=this._vMap;for(;r.has(i);)i=r.get(i),n(i,e)}}};function N(t,n,e,i,r,o,s,a){Zr("NgControlFlow");let c=ne(),l=Qe(),u=Pn(l.consts,o);return Xs(c,l,t,n,e,i,r,u,256,s,a),rv}function rv(t,n,e,i,r,o,s,a){Zr("NgControlFlow");let c=ne(),l=Qe(),u=Pn(l.consts,o);return Xs(c,l,t,n,e,i,r,u,512,s,a),rv}function P(t,n){Zr("NgControlFlow");let e=ne(),i=ji(),r=e[i]!==_n?e[i]:-1,o=r!==-1?Ku(e,at+r):void 0,s=0;if(gn(e,i,t)){let a=te(null);try{if(o!==void 0&&ow(o,s),t!==-1){let c=at+t,l=Ku(e,c),u=h_(e[ie],c),m=aw(l,u,e),g=Zc(e,u,n,{dehydratedView:m});Qc(l,g,s,Qs(u,m))}}finally{te(a)}}else if(o!==void 0){let a=rw(o,s);a!==void 0&&(a[kt]=n)}}var u_=class{lContainer;$implicit;$index;constructor(n,e,i){this.lContainer=n,this.$implicit=e,this.$index=i}get $count(){return this.lContainer.length-_t}};function vi(t){return t}function ns(t,n){return n}var m_=class{hasEmptyBlock;trackByFn;liveCollection;constructor(n,e,i){this.hasEmptyBlock=n,this.trackByFn=e,this.liveCollection=i}};function tt(t,n,e,i,r,o,s,a,c,l,u,m,g){Zr("NgControlFlow");let v=ne(),x=Qe(),F=c!==void 0,j=ne(),Y=a?s.bind(j[pn][kt]):s,be=new m_(F,Y);j[at+t]=be,Xs(v,x,t+1,n,e,i,r,Pn(x.consts,o),256),F&&Xs(v,x,t+2,c,l,u,m,Pn(x.consts,g),512)}var f_=class extends l_{lContainer;hostLView;templateTNode;operationsCounter=void 0;needsIndexUpdate=!1;constructor(n,e,i){super(),this.lContainer=n,this.hostLView=e,this.templateTNode=i}get length(){return this.lContainer.length-_t}at(n){return this.getLView(n)[kt].$implicit}attach(n,e){let i=e[Uo];this.needsIndexUpdate||=n!==this.length,Qc(this.lContainer,e,n,Qs(this.templateTNode,i)),M1(this.lContainer,n)}detach(n){return this.needsIndexUpdate||=n!==this.length-1,T1(this.lContainer,n),A1(this.lContainer,n)}create(n,e){let i=$u(this.lContainer,this.templateTNode.tView.ssrId);return Zc(this.hostLView,this.templateTNode,new u_(this.lContainer,e,n),{dehydratedView:i})}destroy(n){am(n[ie],n)}updateValue(n,e){this.getLView(n)[kt].$implicit=e}reset(){this.needsIndexUpdate=!1}updateIndexes(){if(this.needsIndexUpdate)for(let n=0;n<this.length;n++)this.getLView(n)[kt].$index=n}getLView(n){return R1(this.lContainer,n)}};function nt(t){let n=te(null),e=Bi();try{let i=ne(),r=i[ie],o=i[e],s=e+1,a=Ku(i,s);if(o.liveCollection===void 0){let l=h_(r,s);o.liveCollection=new f_(a,i,l)}else o.liveCollection.reset();let c=o.liveCollection;if(k1(c,t,o.trackByFn,n),c.updateIndexes(),o.hasEmptyBlock){let l=ji(),u=c.length===0;if(gn(i,l,u)){let m=e+2,g=Ku(i,m);if(u){let v=h_(r,m),x=aw(g,v,i),F=Zc(i,v,void 0,{dehydratedView:x});Qc(g,F,0,Qs(v,x))}else r.firstUpdatePass&&kR(g),ow(g,0)}}}finally{te(n)}}function Ku(t,n){return t[n]}function M1(t,n){if(t.length<=_t)return;let e=_t+n,i=t[e],r=i?i[$r]:void 0;if(i&&r&&r.detachedLeaveAnimationFns&&r.detachedLeaveAnimationFns.length>0){let o=i[lr];jA(o,r),Xo.delete(i[dr]),r.detachedLeaveAnimationFns=void 0}}function T1(t,n){if(t.length<=_t)return;let e=_t+n,i=t[e],r=i?i[$r]:void 0;r&&r.leave&&r.leave.size>0&&(r.detachedLeaveAnimationFns=[])}function A1(t,n){return Vc(t,n)}function R1(t,n){return rw(t,n)}function h_(t,n){return gu(t,n)}function D(t,n,e){let i=ne(),r=ji();if(gn(i,r,n)){let o=Qe(),s=$s();$C(s,i,t,n,i[Ue],e)}return D}function p_(t,n,e,i,r){dm(n,t,e,r?"class":"style",i)}function f(t,n,e,i){let r=ne(),o=r[ie],s=t+at,a=o.firstCreatePass?G_(s,r,2,n,V_,yu(),e,i):o.data[s];if(Fi(a)){let c=r[ui].tracingService;if(c&&c.componentCreate){let l=o.data[a.directiveStart+a.componentOffset];return c.componentCreate(hw(l),()=>(jx(t,n,r,a,i),f))}}return jx(t,n,r,a,i),f}function jx(t,n,e,i,r){if(H_(i,e,t,n,Tw),Hs(i)){let o=e[ie];lm(o,e,i),C_(o,i,e)}r!=null&&Yc(e,i)}function h(){let t=Qe(),n=jt(),e=U_(n);return t.firstCreatePass&&W_(t,e),ug(e)&&mg(),lg(),e.classesWithoutHost!=null&&bT(e)&&p_(t,e,ne(),e.classesWithoutHost,!0),e.stylesWithoutHost!=null&&yT(e)&&p_(t,e,ne(),e.stylesWithoutHost,!1),h}function O(t,n,e,i){return f(t,n,e,i),h(),O}function Fe(t,n,e,i){let r=ne(),o=r[ie],s=t+at,a=o.firstCreatePass?UR(s,o,2,n,e,i):o.data[s];return H_(a,r,t,n,Tw),i!=null&&Yc(r,a),Fe}function Ge(){let t=jt(),n=U_(t);return ug(n)&&mg(),lg(),Ge}function Ht(t,n,e,i){return Fe(t,n,e,i),Ge(),Ht}var Tw=(t,n,e,i,r)=>(Tc(!0),DC(n[Ue],i,bg()));function ov(t,n,e){let i=ne(),r=i[ie],o=t+at,s=r.firstCreatePass?G_(o,i,8,"ng-container",V_,yu(),n,e):r.data[o];if(H_(s,i,t,"ng-container",O1),Hs(s)){let a=i[ie];lm(a,i,s),C_(a,s,i)}return e!=null&&Yc(i,s),ov}function sv(){let t=Qe(),n=jt(),e=U_(n);return t.firstCreatePass&&W_(t,e),sv}function sn(t,n,e){return ov(t,n,e),sv(),sn}var O1=(t,n,e,i,r)=>(Tc(!0),fA(n[Ue],""));function Mt(){return ne()}function mt(t,n,e){let i=ne(),r=ji();if(gn(i,r,n)){let o=Qe(),s=$s();GC(s,i,t,n,i[Ue],e)}return mt}var Oc=void 0;function N1(t){let n=Math.floor(Math.abs(t)),e=t.toString().replace(/^[^.]*\.?/,"").length;return n===1&&e===0?1:5}var P1=["en",[["a","p"],["AM","PM"]],[["AM","PM"]],[["S","M","T","W","T","F","S"],["Sun","Mon","Tue","Wed","Thu","Fri","Sat"],["Sunday","Monday","Tuesday","Wednesday","Thursday","Friday","Saturday"],["Su","Mo","Tu","We","Th","Fr","Sa"]],Oc,[["J","F","M","A","M","J","J","A","S","O","N","D"],["Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"],["January","February","March","April","May","June","July","August","September","October","November","December"]],Oc,[["B","A"],["BC","AD"],["Before Christ","Anno Domini"]],0,[6,0],["M/d/yy","MMM d, y","MMMM d, y","EEEE, MMMM d, y"],["h:mm\u202Fa","h:mm:ss\u202Fa","h:mm:ss\u202Fa z","h:mm:ss\u202Fa zzzz"],["{1}, {0}",Oc,Oc,Oc],[".",",",";","%","+","-","E","\xD7","\u2030","\u221E","NaN",":"],["#,##0.###","#,##0%","\xA4#,##0.00","#E0"],"USD","$","US Dollar",{},"ltr",N1],Rg=Object.create(null);function vn(t){let n=F1(t),e=Bx(n);if(e)return e;let i=n.split("-")[0];if(e=Bx(i),e)return e;if(i==="en")return P1;throw new R(701,!1)}function Bx(t){return t in Rg||(Rg[t]=Gn.ng&&Gn.ng.common&&Gn.ng.common.locales&&Gn.ng.common.locales[t]),Rg[t]}var ft=(function(t){return t[t.LocaleId=0]="LocaleId",t[t.DayPeriodsFormat=1]="DayPeriodsFormat",t[t.DayPeriodsStandalone=2]="DayPeriodsStandalone",t[t.DaysFormat=3]="DaysFormat",t[t.DaysStandalone=4]="DaysStandalone",t[t.MonthsFormat=5]="MonthsFormat",t[t.MonthsStandalone=6]="MonthsStandalone",t[t.Eras=7]="Eras",t[t.FirstDayOfWeek=8]="FirstDayOfWeek",t[t.WeekendRange=9]="WeekendRange",t[t.DateFormat=10]="DateFormat",t[t.TimeFormat=11]="TimeFormat",t[t.DateTimeFormat=12]="DateTimeFormat",t[t.NumberSymbols=13]="NumberSymbols",t[t.NumberFormats=14]="NumberFormats",t[t.CurrencyCode=15]="CurrencyCode",t[t.CurrencySymbol=16]="CurrencySymbol",t[t.CurrencyName=17]="CurrencyName",t[t.Currencies=18]="Currencies",t[t.Directionality=19]="Directionality",t[t.PluralCase=20]="PluralCase",t[t.ExtraData=21]="ExtraData",t})(ft||{});function F1(t){return t.toLowerCase().replace(/_/g,"-")}var Jc="en-US",L1="USD";var j1=Jc;function Aw(t){typeof t=="string"&&(j1=t.toLowerCase().replace(/_/g,"-"))}function T(t,n,e){let i=ne(),r=Qe(),o=jt();return Rw(r,i,i[Ue],o,t,n,e),T}function na(t,n,e){let i=ne(),r=Qe(),o=jt();return(o.type&3||e)&&mw(o,r,i,e,i[Ue],t,n,Lu(o,i,n)),na}function Rw(t,n,e,i,r,o,s){let a=!0,c=null;if((i.type&3||s)&&(c??=Lu(i,n,o),mw(i,t,n,s,e,r,o,c)&&(a=!1)),a){let l=i.outputs?.[r],u=i.hostDirectiveOutputs?.[r];if(u&&u.length)for(let m=0;m<u.length;m+=2){let g=u[m],v=u[m+1];c??=Lu(i,n,o),Rx(i,n,g,v,r,c)}if(l&&l.length)for(let m of l)c??=Lu(i,n,o),Rx(i,n,m,r,r,c)}}function S(t=1){return tx(t)}function B1(t,n){let e=null,i=wA(t);for(let r=0;r<n.length;r++){let o=n[r];if(o==="*"){e=r;continue}if(i===null?RC(t,o,!0):SA(i,o))return r}return e}function Ce(t){let n=ne()[pn][hn];if(!n.projection){let e=t?t.length:1,i=n.projection=I0(e,null),r=i.slice(),o=n.child;for(;o!==null;){if(o.type!==128){let s=t?B1(o,t):0;s!==null&&(r[s]?r[s].projectionNext=o:i[s]=o,r[s]=o)}o=o.next}}}function re(t,n=0,e,i,r,o){let s=ne(),a=Qe(),c=i?t+1:null;c!==null&&Xs(s,a,c,i,r,o,null,e);let l=ea(a,at+t,16,null,e||null);l.projection===null&&(l.projection=n),pg();let m=!s[Uo]||dg();s[pn][hn].projection[l.projection]===null&&c!==null?V1(s,a,c):m&&!nm(l)&&ZA(a,s,l)}function V1(t,n,e){let i=at+e,r=n.data[i],o=t[i],s=$u(o,r.tView.ssrId),a=Zc(t,r,void 0,{dehydratedView:s});Qc(o,a,0,Qs(r,s))}function yt(t,n,e,i){return yw(t,n,e,i),yt}function Ve(t,n,e){return bw(t,n,e),Ve}function $(t){let n=ne(),e=Qe(),i=wu();Mc(i+1);let r=Q_(e,i);if(t.dirty&&L0(n)===((r.metadata.flags&2)===2)){if(r.matches===null)t.reset([]);else{let o=Cw(n,i);t.reset(o,sC),t.notifyOnChanges()}return!0}return!1}function G(){return Z_(ne(),wu())}function bm(t,n,e,i,r){return Dw(n,yw(t,e,i,r)),bm}function ym(t,n,e,i){return Dw(t,bw(n,e,i)),ym}function xm(t=1){Mc(wu()+t)}function Re(t){let n=G0();return _u(n,at+t)}function Ru(t,n){return t<<17|n<<2}function Jo(t){return t>>17&32767}function H1(t){return(t&2)==2}function U1(t,n){return t&131071|n<<17}function g_(t){return t|2}function Ks(t){return(t&131068)>>2}function Og(t,n){return t&-131069|n<<2}function z1(t){return(t&1)===1}function __(t){return t|1}function $1(t,n,e,i,r,o){let s=o?n.classBindings:n.styleBindings,a=Jo(s),c=Ks(s);t[i]=e;let l=!1,u;if(Array.isArray(e)){let m=e;u=m[1],(u===null||Ls(m,u)>0)&&(l=!0)}else u=e;if(r)if(c!==0){let g=Jo(t[a+1]);t[i+1]=Ru(g,a),g!==0&&(t[g+1]=Og(t[g+1],i)),t[a+1]=U1(t[a+1],i)}else t[i+1]=Ru(a,0),a!==0&&(t[a+1]=Og(t[a+1],i)),a=i;else t[i+1]=Ru(c,0),a===0?a=i:t[c+1]=Og(t[c+1],i),c=i;l&&(t[i+1]=g_(t[i+1])),Vx(t,u,i,!0),Vx(t,u,i,!1),G1(n,u,t,i,o),s=Ru(a,c),o?n.classBindings=s:n.styleBindings=s}function G1(t,n,e,i,r){let o=r?t.residualClasses:t.residualStyles;o!=null&&typeof n=="string"&&Ls(o,n)>=0&&(e[i+1]=__(e[i+1]))}function Vx(t,n,e,i){let r=t[e+1],o=n===null,s=i?Jo(r):Ks(r),a=!1;for(;s!==0&&(a===!1||o);){let c=t[s],l=t[s+1];W1(c,n)&&(a=!0,t[s+1]=i?__(l):g_(l)),s=i?Jo(l):Ks(l)}a&&(t[e+1]=i?g_(r):__(r))}function W1(t,n){return t===null||n==null||(Array.isArray(t)?t[1]:t)===n?!0:Array.isArray(t)&&typeof n=="string"?Ls(t,n)>=0:!1}var hi={textEnd:0,key:0,keyEnd:0,value:0,valueEnd:0};function q1(t){return t.substring(hi.key,hi.keyEnd)}function Y1(t){return Z1(t),Ow(t,Nw(t,0,hi.textEnd))}function Ow(t,n){let e=hi.textEnd;return e===n?-1:(n=hi.keyEnd=Q1(t,hi.key=n,e),Nw(t,n,e))}function Z1(t){hi.key=0,hi.keyEnd=0,hi.value=0,hi.valueEnd=0,hi.textEnd=t.length}function Nw(t,n,e){for(;n<e&&t.charCodeAt(n)<=32;)n++;return n}function Q1(t,n,e){for(;n<e&&t.charCodeAt(n)>32;)n++;return n}function In(t,n,e){return Pw(t,n,e,!1),In}function z(t,n){return Pw(t,n,null,!0),z}function Tt(t){K1(rO,X1,t,!0)}function X1(t,n){for(let e=Y1(n);e>=0;e=Ow(n,e))fu(t,q1(n),!0)}function Pw(t,n,e,i){let r=ne(),o=Qe(),s=xu(2);if(o.firstUpdatePass&&Lw(o,t,s,i),n!==_n&&gn(r,s,n)){let a=o.data[Bi()];jw(o,a,r,r[Ue],t,r[s+1]=sO(n,e),i,s)}}function K1(t,n,e,i){let r=Qe(),o=xu(2);r.firstUpdatePass&&Lw(r,null,o,i);let s=ne();if(e!==_n&&gn(s,o,e)){let a=r.data[Bi()];if(Bw(a,i)&&!Fw(r,o)){let c=i?a.classesWithoutHost:a.stylesWithoutHost;c!==null&&(e=cu(c,e||"")),p_(r,a,s,e,i)}else oO(r,a,s,s[Ue],s[o+1],s[o+1]=iO(t,n,e),i,o)}}function Fw(t,n){return n>=t.expandoStartIndex}function Lw(t,n,e,i){let r=t.data;if(r[e+1]===null){let o=r[Bi()],s=Fw(t,e);Bw(o,i)&&n===null&&!s&&(n=!1),n=J1(r,o,n,i),$1(r,o,n,e,s,i)}}function J1(t,n,e,i){let r=X0(t),o=i?n.residualClasses:n.residualStyles;if(r===null)(i?n.classBindings:n.styleBindings)===0&&(e=Ng(null,t,n,e,i),e=Uc(e,n.attrs,i),o=null);else{let s=n.directiveStylingLast;if(s===-1||t[s]!==r)if(e=Ng(r,t,n,e,i),o===null){let c=eO(t,n,i);c!==void 0&&Array.isArray(c)&&(c=Ng(null,t,n,c[1],i),c=Uc(c,n.attrs,i),tO(t,n,i,c))}else o=nO(t,n,i)}return o!==void 0&&(i?n.residualClasses=o:n.residualStyles=o),e}function eO(t,n,e){let i=e?n.classBindings:n.styleBindings;if(Ks(i)!==0)return t[Jo(i)]}function tO(t,n,e,i){let r=e?n.classBindings:n.styleBindings;t[Jo(r)]=i}function nO(t,n,e){let i,r=n.directiveEnd;for(let o=1+n.directiveStylingLast;o<r;o++){let s=t[o].hostAttrs;i=Uc(i,s,e)}return Uc(i,n.attrs,e)}function Ng(t,n,e,i,r){let o=null,s=e.directiveEnd,a=e.directiveStylingLast;for(a===-1?a=e.directiveStart:a++;a<s&&(o=n[a],i=Uc(i,o.hostAttrs,r),o!==t);)a++;return t!==null&&(e.directiveStylingLast=a),i}function Uc(t,n,e){let i=e?1:2,r=-1;if(n!==null)for(let o=0;o<n.length;o++){let s=n[o];typeof s=="number"?r=s:r===i&&(Array.isArray(t)||(t=t===void 0?[]:["",t]),fu(t,s,e?!0:n[++o]))}return t===void 0?null:t}function iO(t,n,e){if(e==null||e==="")return nn;let i=[],r=gi(e);if(Array.isArray(r))for(let o=0;o<r.length;o++)t(i,r[o],!0);else if(r instanceof Set)for(let o of r)t(i,o,!0);else if(typeof r=="object")for(let o in r)r.hasOwnProperty(o)&&t(i,o,r[o]);else typeof r=="string"&&n(i,r);return i}function rO(t,n,e){let i=String(n);i!==""&&!i.includes(" ")&&fu(t,i,e)}function oO(t,n,e,i,r,o,s,a){r===_n&&(r=nn);let c=0,l=0,u=0<r.length?r[0]:null,m=0<o.length?o[0]:null;for(;u!==null||m!==null;){let g=c<r.length?r[c+1]:void 0,v=l<o.length?o[l+1]:void 0,x=null,F;u===m?(c+=2,l+=2,g!==v&&(x=m,F=v)):m===null||u!==null&&u<m?(c+=2,x=u):(l+=2,x=m,F=v),x!==null&&jw(t,n,e,i,x,F,s,a),u=c<r.length?r[c]:null,m=l<o.length?o[l]:null}}function jw(t,n,e,i,r,o,s,a){if(!(n.type&3))return;let c=t.data,l=c[a+1],u=z1(l)?Hx(c,n,e,r,Ks(l),s):void 0;if(!Ju(u)){Ju(o)||H1(l)&&(o=Hx(c,null,e,r,a,s));let m=ng(Bi(),e);XA(i,s,m,r,o)}}function Hx(t,n,e,i,r,o){let s=n===null,a;for(;r>0;){let c=t[r],l=Array.isArray(c),u=l?c[1]:c,m=u===null,g=e[r+1];g===_n&&(g=m?nn:void 0);let v=m?hu(g,i):u===i?g:void 0;if(l&&!Ju(v)&&(v=hu(c,i)),Ju(v)&&(a=v,s))return a;let x=t[r+1];r=s?Jo(x):Ks(x)}if(n!==null){let c=o?n.residualClasses:n.residualStyles;c!=null&&(a=hu(c,i))}return a}function Ju(t){return t!==void 0}function sO(t,n){return t==null||t===""||(typeof n=="string"?t=t+n:typeof t=="object"&&(t=yc(gi(t)))),t}function Bw(t,n){return(t.flags&(n?8:16))!==0}function _(t,n=""){let e=ne(),i=Qe(),r=t+at,o=i.firstCreatePass?ea(i,r,1,n,null):i.data[r],s=aO(i,e,o,n);e[r]=s,Su()&&j_(i,e,s,o),zs(o,!1)}var aO=(t,n,e,i)=>(Tc(!0),uA(n[Ue],i));function Vw(t,n,e,i=""){return gn(t,ji(),e)?n+Ho(e)+i:_n}function cO(t,n,e,i,r,o=""){let s=W0(),a=Y_(t,s,e,r);return xu(2),a?n+Ho(e)+i+Ho(r)+o:_n}function V(t){return we("",t),V}function we(t,n,e){let i=ne(),r=Vw(i,t,n,e);return r!==_n&&Hw(i,Bi(),r),we}function ia(t,n,e,i,r){let o=ne(),s=cO(o,t,n,e,i,r);return s!==_n&&Hw(o,Bi(),s),ia}function Hw(t,n,e){let i=ng(n,t);mA(t[Ue],i,e)}function Kr(t,n,e){J_(n)&&(n=n());let i=ne(),r=ji();if(gn(i,r,n)){let o=Qe(),s=$s();$C(s,i,t,n,i[Ue],e)}return Kr}function is(t,n){let e=J_(t);return e&&t.set(n),e}function Jr(t,n){let e=ne(),i=Qe(),r=jt();return Rw(i,e,e[Ue],r,t,n),Jr}function an(t){return gn(ne(),ji(),t)?Ho(t):_n}function Ln(t,n,e=""){return Vw(ne(),t,n,e)}function Ux(t,n,e){let i=Qe();i.firstCreatePass&&Uw(n,i.data,i.blueprint,Li(t),e)}function Uw(t,n,e,i,r){if(t=Qt(t),Array.isArray(t))for(let o=0;o<t.length;o++)Uw(t[o],n,e,i,r);else{let o=Qe(),s=ne(),a=jt(),c=jo(t)?t:Qt(t.provide),l=Qp(t),u=a.providerIndexes&1048575,m=a.directiveStart,g=a.providerIndexes>>20;if(jo(t)||!t.multi){let v=new Qo(l,r,ee,null),x=Fg(c,n,r?u:u+g,m);x===-1?(jg(Uu(a,s),o,c),Pg(o,t,n.length),n.push(c),a.directiveStart++,a.directiveEnd++,r&&(a.providerIndexes+=1048576),e.push(v),s.push(v)):(e[x]=v,s[x]=v)}else{let v=Fg(c,n,u+g,m),x=Fg(c,n,u,u+g),F=v>=0&&e[v],j=x>=0&&e[x];if(r&&!j||!r&&!F){jg(Uu(a,s),o,c);let Y=uO(r?dO:lO,e.length,r,i,l,t);!r&&j&&(e[x].providerFactory=Y),Pg(o,t,n.length,0),n.push(c),a.directiveStart++,a.directiveEnd++,r&&(a.providerIndexes+=1048576),e.push(Y),s.push(Y)}else{let Y=zw(e[r?x:v],l,!r&&i);Pg(o,t,v>-1?v:x,Y)}!r&&i&&j&&e[x].componentProviders++}}}function Pg(t,n,e,i){let r=jo(n),o=O0(n);if(r||o){let c=(o?Qt(n.useClass):n).prototype.ngOnDestroy;if(c){let l=t.destroyHooks||(t.destroyHooks=[]);if(!r&&n.multi){let u=l.indexOf(e);u===-1?l.push(e,[i,c]):l[u+1].push(i,c)}else l.push(e,c)}}}function zw(t,n,e){return e&&t.componentProviders++,t.multi.push(n)-1}function Fg(t,n,e,i){for(let r=e;r<i;r++)if(n[r]===t)return r;return-1}function lO(t,n,e,i,r){return v_(this.multi,[])}function dO(t,n,e,i,r){let o=this.multi,s;if(this.providerFactory){let a=this.providerFactory.componentProviders,c=Lc(i,i[ie],this.providerFactory.index,r);s=c.slice(0,a),v_(o,s);for(let l=a;l<c.length;l++)s.push(c[l])}else s=[],v_(o,s);return s}function v_(t,n){for(let e=0;e<t.length;e++){let i=t[e];n.push(i())}return n}function uO(t,n,e,i,r,o){let s=new Qo(t,e,ee,null);return s.multi=[],s.index=n,s.componentProviders=0,zw(s,r,i&&!e),s}function Ee(t,n){return e=>{e.providersResolver=(i,r)=>Ux(i,r?r(t):t,!1),n&&(e.viewProvidersResolver=(i,r)=>Ux(i,r?r(n):n,!0))}}function ra(t,n,e){return $w(ne(),kc(),t,n,e)}function av(t,n,e,i,r){return fO(ne(),kc(),t,n,e,i,r)}function cv(t,n){let e=t[n];return e===_n?void 0:e}function $w(t,n,e,i,r,o){let s=n+e;return gn(t,s,r)?q_(t,s+1,o?i.call(o,r):i(r)):cv(t,s+1)}function mO(t,n,e,i,r,o,s){let a=n+e;return Y_(t,a,r,o)?q_(t,a+2,s?i.call(s,r,o):i(r,o)):cv(t,a+2)}function fO(t,n,e,i,r,o,s,a){let c=n+e;return zR(t,c,r,o,s)?q_(t,c+3,a?i.call(a,r,o,s):i(r,o,s)):cv(t,c+3)}function Se(t,n){let e=Qe(),i,r=t+at;e.firstCreatePass?(i=hO(n,e.pipeRegistry),e.data[r]=i,i.onDestroy&&(e.destroyHooks??=[]).push(r,i.onDestroy)):i=e.data[r];let o=i.factory||(i.factory=jr(i.type,!0)),s,a=fn(ee);try{let c=Hu(!1),l=o();return Hu(c),ig(e,ne(),r,l),l}finally{fn(a)}}function hO(t,n){if(n)for(let e=n.length-1;e>=0;e--){let i=n[e];if(t===i.name)return i}}function Oe(t,n,e){let i=t+at,r=ne(),o=_u(r,i);return Gw(r,i)?$w(r,kc(),n,o.transform,e,o):o.transform(e)}function el(t,n,e,i){let r=t+at,o=ne(),s=_u(o,r);return Gw(o,r)?mO(o,kc(),n,s.transform,e,i,s):s.transform(e,i)}function Gw(t,n){return t[ie].data[n].pure}function Xn(t,n){return um(t,n)}var em=class{ngModuleFactory;componentFactories;constructor(n,e){this.ngModuleFactory=n,this.componentFactories=e}},lv=(()=>{class t{compileModuleSync(e){return new Qu(e)}compileModuleAsync(e){return Promise.resolve(this.compileModuleSync(e))}compileModuleAndAllComponentsSync(e){let i=this.compileModuleSync(e),r=Hp(e),o=TC(r.declarations).reduce((s,a)=>{let c=cr(a);return c&&s.push(new Ko(c)),s},[]);return new em(i,o)}compileModuleAndAllComponentsAsync(e){return Promise.resolve(this.compileModuleAndAllComponentsSync(e))}clearCache(){}clearCacheFor(e){}getModuleId(e){}static \u0275fac=function(i){return new(i||t)};static \u0275prov=C({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();var Ww=(()=>{class t{applicationErrorHandler=d(Fn);appRef=d(Sn);taskService=d(mr);ngZone=d(H);zonelessEnabled=d(Ac);tracing=d(_i,{optional:!0});zoneIsDefined=typeof Zone<"u"&&!!Zone.root.run;schedulerTickApplyArgs=[{data:{__scheduler_tick__:!0}}];subscriptions=new ge;angularZoneId=this.zoneIsDefined?this.ngZone._inner?.get(vc):null;scheduleInRootZone=!this.zonelessEnabled&&this.zoneIsDefined&&(d(Eg,{optional:!0})??!1);cancelScheduledCallback=null;useMicrotaskScheduler=!1;runningTick=!1;pendingRenderTaskId=null;constructor(){this.subscriptions.add(this.appRef.afterTick.subscribe(()=>{let e=this.taskService.add();if(!this.runningTick&&(this.cleanup(),!this.zonelessEnabled||this.appRef.includeAllTestViews)){this.taskService.remove(e);return}this.switchToMicrotaskScheduler(),this.taskService.remove(e)})),this.subscriptions.add(this.ngZone.onUnstable.subscribe(()=>{this.runningTick||this.cleanup()}))}switchToMicrotaskScheduler(){this.ngZone.runOutsideAngular(()=>{let e=this.taskService.add();this.useMicrotaskScheduler=!0,queueMicrotask(()=>{this.useMicrotaskScheduler=!1,this.taskService.remove(e)})})}notify(e){if(!this.zonelessEnabled&&e===5)return;switch(e){case 0:{this.appRef.dirtyFlags|=2;break}case 3:case 2:case 4:case 5:case 1:{this.appRef.dirtyFlags|=4;break}case 6:{this.appRef.dirtyFlags|=2;break}case 12:{this.appRef.dirtyFlags|=16;break}case 13:{this.appRef.dirtyFlags|=2;break}case 11:break;default:this.appRef.dirtyFlags|=8}if(this.appRef.tracingSnapshot=this.tracing?.snapshot(this.appRef.tracingSnapshot)??null,!this.shouldScheduleTick())return;let i=this.useMicrotaskScheduler?ox:xg;this.pendingRenderTaskId=this.taskService.add(),this.scheduleInRootZone?this.cancelScheduledCallback=Zone.root.run(()=>i(()=>this.tick())):this.cancelScheduledCallback=this.ngZone.runOutsideAngular(()=>i(()=>this.tick()))}shouldScheduleTick(){return!(this.appRef.destroyed||this.pendingRenderTaskId!==null||this.runningTick||this.appRef._runningTick||!this.zonelessEnabled&&this.zoneIsDefined&&Zone.current.get(vc+this.angularZoneId))}tick(){if(this.runningTick||this.appRef.destroyed)return;if(this.appRef.dirtyFlags===0){this.cleanup();return}!this.zonelessEnabled&&this.appRef.dirtyFlags&7&&(this.appRef.dirtyFlags|=1);let e=this.taskService.add();try{this.ngZone.run(()=>{this.runningTick=!0,this.appRef._tick()},void 0,this.schedulerTickApplyArgs)}catch(i){this.applicationErrorHandler(i)}finally{this.taskService.remove(e),this.cleanup()}}ngOnDestroy(){this.subscriptions.unsubscribe(),this.cleanup()}cleanup(){if(this.runningTick=!1,this.cancelScheduledCallback?.(),this.cancelScheduledCallback=null,this.pendingRenderTaskId!==null){let e=this.pendingRenderTaskId;this.pendingRenderTaskId=null,this.taskService.remove(e)}}static \u0275fac=function(i){return new(i||t)};static \u0275prov=C({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();function qw(){return[{provide:Ri,useExisting:Ww},{provide:H,useClass:bc},{provide:Ac,useValue:!0}]}function pO(){return typeof $localize<"u"&&$localize.locale||Jc}var oa=new y("",{factory:()=>d(oa,{optional:!0,skipSelf:!0})||pO()}),dv=new y("",{factory:()=>L1});var Cm=class{destroyed=!1;listeners=null;errorHandler=d(rn,{optional:!0});destroyRef=d(on);constructor(){this.destroyRef.onDestroy(()=>{this.destroyed=!0,this.listeners=null})}subscribe(n){if(this.destroyed)throw new R(953,!1);return(this.listeners??=[]).push(n),{unsubscribe:()=>{let e=this.listeners?.indexOf(n);e!==void 0&&e!==-1&&this.listeners?.splice(e,1)}}}emit(n){if(this.destroyed){console.warn(Oi(953,!1));return}if(this.listeners===null)return;let e=te(null);try{for(let i of this.listeners)try{i(n)}catch(r){this.errorHandler?.handleError(r)}}finally{te(e)}}};function Ne(t){return p0(t)}function xt(t,n){return ic(t,n?.equal)}var gO=t=>t;function uv(t,n){if(typeof t=="function"){let e=wp(t,gO,n?.equal);return Yw(e,n?.debugName)}else{let e=wp(t.source,t.computation,t.equal);return Yw(e,t.debugName)}}function Yw(t,n){let e=t[Et],i=t;return i.set=r=>f0(e,r),i.update=r=>h0(e,r),i.asReadonly=Iu.bind(t),i}var eD=Symbol("InputSignalNode#UNSET"),MO=Z(b({},rc),{transformFn:void 0,applyValueToInputSignal(t,n){So(t,n)}});function tD(t,n){let e=Object.create(MO);e.value=t,e.transformFn=n?.transform;function i(){if(Or(e),e.value===eD){let r=null;throw new R(-950,r)}return e.value}return i[Et]=e,i}var Kn=class{attributeName;constructor(n){this.attributeName=n}__NG_ELEMENT_ID__=()=>$c(this.attributeName);toString(){return`HostAttributeToken ${this.attributeName}`}};function nD(t){return new Cm}function Zw(t,n){return tD(t,n)}function TO(t){return tD(eD,t)}var sa=(Zw.required=TO,Zw);function Qw(t,n){return X_(n)}function AO(t,n){return K_(n)}var nl=(Qw.required=AO,Qw);function Xw(t,n){return X_(n)}function RO(t,n){return K_(n)}var iD=(Xw.required=RO,Xw);var fv=new y(""),OO=new y("");function tl(t){return!t.moduleRef}function NO(t){let n=tl(t)?t.r3Injector:t.moduleRef.injector,e=n.get(H);return e.run(()=>{tl(t)?t.r3Injector.resolveInjectorInitializers():t.moduleRef.resolveInjectorInitializers();let i=n.get(Fn),r;if(e.runOutsideAngular(()=>{r=e.onError.subscribe({next:i})}),tl(t)){let o=()=>n.destroy(),s=t.platformInjector.get(fv);s.add(o),n.onDestroy(()=>{r.unsubscribe(),s.delete(o)})}else{let o=()=>t.moduleRef.destroy(),s=t.platformInjector.get(fv);s.add(o),t.moduleRef.onDestroy(()=>{Fc(t.allPlatformModules,t.moduleRef),r.unsubscribe(),s.delete(o)})}return FO(i,e,()=>{let o=n.get(mr),s=o.add(),a=n.get(iv);return a.runInitializers(),a.donePromise.then(()=>{let c=n.get(oa,Jc);if(Aw(c||Jc),!n.get(OO,!0))return tl(t)?n.get(Sn):(t.allPlatformModules.push(t.moduleRef),t.moduleRef);if(tl(t)){let u=n.get(Sn);return t.rootComponent!==void 0&&u.bootstrap(t.rootComponent),u}else return PO?.(t.moduleRef,t.allPlatformModules),t.moduleRef}).finally(()=>{o.remove(s)})})})}var PO;function FO(t,n,e){try{let i=e();return Xr(i)?i.catch(r=>{throw n.runOutsideAngular(()=>t(r)),r}):i}catch(i){throw n.runOutsideAngular(()=>t(i)),i}}var wm=null;function LO(t=[],n){return X.create({name:n,providers:[{provide:Ec,useValue:"platform"},{provide:fv,useValue:new Set([()=>wm=null])},...t]})}function jO(t=[]){if(wm)return wm;let n=LO(t);return wm=n,kw(),BO(n),n}function BO(t){let n=t.get(tm,null);$t(t,()=>{n?.forEach(e=>e())})}var VO=1e4;var sZ=VO-1e3;var ve=(()=>{class t{static __NG_ELEMENT_ID__=HO}return t})();function HO(t){return UO(jt(),ne(),(t&16)===16)}function UO(t,n,e){if(Fi(t)&&!e){let i=Zn(t.index,n);return new qr(i,i)}else if(t.type&175){let i=n[pn];return new qr(i,n)}return null}function rD(t){let{rootComponent:n,appProviders:e,platformProviders:i,platformRef:r}=t;Be(Me.BootstrapApplicationStart);try{let o=r?.injector??jO(i),s=[qw(),ax,...e||[]],a=new Hc({providers:s,parent:o,debugName:"",runEnvironmentInitializers:!1});return NO({r3Injector:a.injector,platformInjector:o,rootComponent:n})}catch(o){return Promise.reject(o)}finally{Be(Me.BootstrapApplicationEnd)}}function B(t){return typeof t=="boolean"?t:t!=null&&t!=="false"}function lt(t,n=NaN){return!isNaN(parseFloat(t))&&!isNaN(Number(t))?Number(t):n}var mv=Symbol("NOT_SET"),oD=new Set,zO=Z(b({},rc),{kind:"afterRenderEffectPhase",consumerIsAlwaysLive:!0,consumerAllowSignalWrites:!0,value:mv,cleanup:null,consumerMarkedDirty(){if(this.sequence.impl.executing){if(this.sequence.lastPhase===null||this.sequence.lastPhase<this.phase)return;this.sequence.erroredOrDestroyed=!0}this.sequence.scheduler.notify(7)},phaseFn(t){if(this.sequence.lastPhase=this.phase,!this.dirty)return this.signal;if(this.dirty=!1,this.value!==mv&&!ks(this))return this.signal;try{for(let r of this.cleanup??oD)r()}finally{this.cleanup?.clear()}let n=[];t!==void 0&&n.push(t),n.push(this.registerCleanupFn);let e=rr(this),i;try{i=this.userFn.apply(null,n)}finally{Nr(this,e)}return(this.value===mv||!this.equal(this.value,i))&&(this.value=i,this.version++),this.signal}}),hv=class extends jc{scheduler;lastPhase=null;nodes=[void 0,void 0,void 0,void 0];onDestroyFns=null;constructor(n,e,i,r,o,s=null){super(n,[void 0,void 0,void 0,void 0],i,!1,o.get(on),s),this.scheduler=r;for(let a of P_){let c=e[a];if(c===void 0)continue;let l=Object.create(zO);l.sequence=this,l.phase=a,l.userFn=c,l.dirty=!0,l.signal=()=>(Or(l),l.value),l.signal[Et]=l,l.registerCleanupFn=u=>(l.cleanup??=new Set).add(u),this.nodes[a]=l,this.hooks[a]=u=>l.phaseFn(u)}}afterRun(){super.afterRun(),this.lastPhase=null}destroy(){if(this.onDestroyFns!==null)for(let n of this.onDestroyFns)n();super.destroy();for(let n of this.nodes)if(n)try{for(let e of n.cleanup??oD)e()}finally{Pr(n)}}};function sD(t,n){let e=n?.injector??d(X),i=e.get(Ri),r=e.get(sm),o=e.get(_i,null,{optional:!0});r.impl??=e.get(F_);let s=t;typeof s=="function"&&(s={mixedReadWrite:t});let a=e.get(Gs,null,{optional:!0}),c=new hv(r.impl,[s.earlyRead,s.write,s.mixedReadWrite,s.read],a?.view,i,e,o?.snapshot(null));return r.impl.register(c),c}function Dm(t,n){let e=cr(t),i=n.elementInjector||js();return new Ko(e).create(i,n.projectableNodes,n.hostElement,n.environmentInjector,n.directives,n.bindings)}var aD=null;function Jn(){return aD}function pv(t){aD??=t}var il=class{},aa=(()=>{class t{historyGo(e){throw new Error("")}static \u0275fac=function(i){return new(i||t)};static \u0275prov=C({token:t,factory:()=>d(cD),providedIn:"platform"})}return t})();var cD=(()=>{class t extends aa{_location;_history;_doc=d(K);constructor(){super(),this._location=window.location,this._history=window.history}getBaseHrefFromDOM(){return Jn().getBaseHref(this._doc)}onPopState(e){let i=Jn().getGlobalEventTarget(this._doc,"window");return i.addEventListener("popstate",e,!1),()=>i.removeEventListener("popstate",e)}onHashChange(e){let i=Jn().getGlobalEventTarget(this._doc,"window");return i.addEventListener("hashchange",e,!1),()=>i.removeEventListener("hashchange",e)}get href(){return this._location.href}get protocol(){return this._location.protocol}get hostname(){return this._location.hostname}get port(){return this._location.port}get pathname(){return this._location.pathname}get search(){return this._location.search}get hash(){return this._location.hash}set pathname(e){this._location.pathname=e}pushState(e,i,r){this._history.pushState(e,i,r)}replaceState(e,i,r){this._history.replaceState(e,i,r)}forward(){this._history.forward()}back(){this._history.back()}historyGo(e=0){this._history.go(e)}getState(){return this._history.state}static \u0275fac=function(i){return new(i||t)};static \u0275prov=C({token:t,factory:()=>new t,providedIn:"platform"})}return t})();function uD(t,n){return t?n?t.endsWith("/")?n.startsWith("/")?t+n.slice(1):t+n:n.startsWith("/")?t+n:`${t}/${n}`:t:n}function lD(t){let n=t.search(/#|\?|$/);return t[n-1]==="/"?t.slice(0,n-1)+t.slice(n):t}function eo(t){return t&&t[0]!=="?"?`?${t}`:t}var ca=(()=>{class t{historyGo(e){throw new Error("")}static \u0275fac=function(i){return new(i||t)};static \u0275prov=C({token:t,factory:()=>d(GO),providedIn:"root"})}return t})(),$O=new y(""),GO=(()=>{class t extends ca{_platformLocation;_baseHref;_removeListenerFns=[];constructor(e,i){super(),this._platformLocation=e,this._baseHref=i??this._platformLocation.getBaseHrefFromDOM()??d(K).location?.origin??""}ngOnDestroy(){for(;this._removeListenerFns.length;)this._removeListenerFns.pop()()}onPopState(e){this._removeListenerFns.push(this._platformLocation.onPopState(e),this._platformLocation.onHashChange(e))}getBaseHref(){return this._baseHref}prepareExternalUrl(e){return uD(this._baseHref,e)}path(e=!1){let i=this._platformLocation.pathname+eo(this._platformLocation.search),r=this._platformLocation.hash;return r&&e?`${i}${r}`:i}pushState(e,i,r,o){let s=this.prepareExternalUrl(r+eo(o));this._platformLocation.pushState(e,i,s)}replaceState(e,i,r,o){let s=this.prepareExternalUrl(r+eo(o));this._platformLocation.replaceState(e,i,s)}forward(){this._platformLocation.forward()}back(){this._platformLocation.back()}getState(){return this._platformLocation.getState()}historyGo(e=0){this._platformLocation.historyGo?.(e)}static \u0275fac=function(i){return new(i||t)(J(aa),J($O,8))};static \u0275prov=C({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();var Gi=(()=>{class t{_subject=new I;_basePath;_locationStrategy;_urlChangeListeners=[];_urlChangeSubscription=null;constructor(e){this._locationStrategy=e;let i=this._locationStrategy.getBaseHref();this._basePath=YO(lD(dD(i))),this._locationStrategy.onPopState(r=>{this._subject.next({url:this.path(!0),pop:!0,state:r.state,type:r.type})})}ngOnDestroy(){this._urlChangeSubscription?.unsubscribe(),this._urlChangeListeners=[]}path(e=!1){return this.normalize(this._locationStrategy.path(e))}getState(){return this._locationStrategy.getState()}isCurrentPathEqualTo(e,i=""){return this.path()==this.normalize(e+eo(i))}normalize(e){return t.stripTrailingSlash(qO(this._basePath,dD(e)))}prepareExternalUrl(e){return e&&e[0]!=="/"&&(e="/"+e),this._locationStrategy.prepareExternalUrl(e)}go(e,i="",r=null){this._locationStrategy.pushState(r,"",e,i),this._notifyUrlChangeListeners(this.prepareExternalUrl(e+eo(i)),r)}replaceState(e,i="",r=null){this._locationStrategy.replaceState(r,"",e,i),this._notifyUrlChangeListeners(this.prepareExternalUrl(e+eo(i)),r)}forward(){this._locationStrategy.forward()}back(){this._locationStrategy.back()}historyGo(e=0){this._locationStrategy.historyGo?.(e)}onUrlChange(e){return this._urlChangeListeners.push(e),this._urlChangeSubscription??=this.subscribe(i=>{this._notifyUrlChangeListeners(i.url,i.state)}),()=>{let i=this._urlChangeListeners.indexOf(e);this._urlChangeListeners.splice(i,1),this._urlChangeListeners.length===0&&(this._urlChangeSubscription?.unsubscribe(),this._urlChangeSubscription=null)}}_notifyUrlChangeListeners(e="",i){this._urlChangeListeners.forEach(r=>r(e,i))}subscribe(e,i,r){return this._subject.subscribe({next:e,error:i??void 0,complete:r??void 0})}static normalizeQueryParams=eo;static joinWithSlash=uD;static stripTrailingSlash=lD;static \u0275fac=function(i){return new(i||t)(J(ca))};static \u0275prov=C({token:t,factory:()=>WO(),providedIn:"root"})}return t})();function WO(){return new Gi(J(ca))}function qO(t,n){if(!t||!n.startsWith(t))return n;let e=n.substring(t.length);return e===""||["/",";","?","#"].includes(e[0])?e:n}function dD(t){return t.replace(/\/index\.html$/,"")}function YO(t){if(new RegExp("^(https?:)?//").test(t)){let[,e]=t.split(/\/\/[^\/]+/);return e}return t}var pD={ADP:[void 0,void 0,0],AFN:[void 0,"\u060B",0],ALL:[void 0,void 0,0],AMD:[void 0,"\u058F",2],AOA:[void 0,"Kz"],ARS:[void 0,"$"],AUD:["A$","$"],AZN:[void 0,"\u20BC"],BAM:[void 0,"KM"],BBD:[void 0,"$"],BDT:[void 0,"\u09F3"],BHD:[void 0,void 0,3],BIF:[void 0,void 0,0],BMD:[void 0,"$"],BND:[void 0,"$"],BOB:[void 0,"Bs"],BRL:["R$"],BSD:[void 0,"$"],BWP:[void 0,"P"],BYN:[void 0,void 0,2],BYR:[void 0,void 0,0],BZD:[void 0,"$"],CAD:["CA$","$",2],CHF:[void 0,void 0,2],CLF:[void 0,void 0,4],CLP:[void 0,"$",0],CNY:["CN\xA5","\xA5"],COP:[void 0,"$",2],CRC:[void 0,"\u20A1",2],CUC:[void 0,"$"],CUP:[void 0,"$"],CZK:[void 0,"K\u010D",2],DJF:[void 0,void 0,0],DKK:[void 0,"kr",2],DOP:[void 0,"$"],EGP:[void 0,"E\xA3"],ESP:[void 0,"\u20A7",0],EUR:["\u20AC"],FJD:[void 0,"$"],FKP:[void 0,"\xA3"],GBP:["\xA3"],GEL:[void 0,"\u20BE"],GHS:[void 0,"GH\u20B5"],GIP:[void 0,"\xA3"],GNF:[void 0,"FG",0],GTQ:[void 0,"Q"],GYD:[void 0,"$",2],HKD:["HK$","$"],HNL:[void 0,"L"],HRK:[void 0,"kn"],HUF:[void 0,"Ft",2],IDR:[void 0,"Rp",2],ILS:["\u20AA"],INR:["\u20B9"],IQD:[void 0,void 0,0],IRR:[void 0,void 0,0],ISK:[void 0,"kr",0],ITL:[void 0,void 0,0],JMD:[void 0,"$"],JOD:[void 0,void 0,3],JPY:["\xA5",void 0,0],KGS:[void 0,"\u20C0"],KHR:[void 0,"\u17DB"],KMF:[void 0,"CF",0],KPW:[void 0,"\u20A9",0],KRW:["\u20A9",void 0,0],KWD:[void 0,void 0,3],KYD:[void 0,"$"],KZT:[void 0,"\u20B8"],LAK:[void 0,"\u20AD",0],LBP:[void 0,"L\xA3",0],LKR:[void 0,"Rs"],LRD:[void 0,"$"],LTL:[void 0,"Lt"],LUF:[void 0,void 0,0],LVL:[void 0,"Ls"],LYD:[void 0,void 0,3],MGA:[void 0,"Ar",0],MGF:[void 0,void 0,0],MMK:[void 0,"K",0],MNT:[void 0,"\u20AE",2],MRO:[void 0,void 0,0],MUR:[void 0,"Rs",2],MXN:["MX$","$"],MYR:[void 0,"RM"],NAD:[void 0,"$"],NGN:[void 0,"\u20A6"],NIO:[void 0,"C$"],NOK:[void 0,"kr",2],NPR:[void 0,"Rs"],NZD:["NZ$","$"],OMR:[void 0,void 0,3],PHP:["\u20B1"],PKR:[void 0,"Rs",2],PLN:[void 0,"z\u0142"],PYG:[void 0,"\u20B2",0],RON:[void 0,"lei"],RSD:[void 0,void 0,0],RUB:[void 0,"\u20BD"],RWF:[void 0,"RF",0],SBD:[void 0,"$"],SEK:[void 0,"kr",2],SGD:[void 0,"$"],SHP:[void 0,"\xA3"],SLE:[void 0,void 0,2],SLL:[void 0,void 0,0],SOS:[void 0,void 0,0],SRD:[void 0,"$"],SSP:[void 0,"\xA3"],STD:[void 0,void 0,0],STN:[void 0,"Db"],SYP:[void 0,"\xA3",0],THB:[void 0,"\u0E3F"],TMM:[void 0,void 0,0],TND:[void 0,void 0,3],TOP:[void 0,"T$"],TRL:[void 0,void 0,0],TRY:[void 0,"\u20BA"],TTD:[void 0,"$"],TWD:["NT$","$",2],TZS:[void 0,void 0,2],UAH:[void 0,"\u20B4"],UGX:[void 0,void 0,0],USD:["$"],UYI:[void 0,void 0,0],UYU:[void 0,"$"],UYW:[void 0,void 0,4],UZS:[void 0,void 0,2],VEF:[void 0,"Bs",2],VND:["\u20AB",void 0,0],VUV:[void 0,void 0,0],XAF:["FCFA",void 0,0],XCD:["EC$","$"],XCG:["Cg."],XOF:["F\u202FCFA",void 0,0],XPF:["CFPF",void 0,0],XXX:["\xA4"],YER:[void 0,void 0,0],ZAR:[void 0,"R"],ZMK:[void 0,void 0,0],ZMW:[void 0,"ZK"],ZWD:[void 0,void 0,0]},xv=(function(t){return t[t.Decimal=0]="Decimal",t[t.Percent=1]="Percent",t[t.Currency=2]="Currency",t[t.Scientific=3]="Scientific",t})(xv||{});var cn=(function(t){return t[t.Format=0]="Format",t[t.Standalone=1]="Standalone",t})(cn||{}),We=(function(t){return t[t.Narrow=0]="Narrow",t[t.Abbreviated=1]="Abbreviated",t[t.Wide=2]="Wide",t[t.Short=3]="Short",t})(We||{}),kn=(function(t){return t[t.Short=0]="Short",t[t.Medium=1]="Medium",t[t.Long=2]="Long",t[t.Full=3]="Full",t})(kn||{}),Mn={Decimal:0,Group:1,List:2,PercentSign:3,PlusSign:4,MinusSign:5,Exponential:6,SuperscriptingExponent:7,PerMille:8,Infinity:9,NaN:10,TimeSeparator:11,CurrencyDecimal:12,CurrencyGroup:13};function gD(t){return vn(t)[ft.LocaleId]}function _D(t,n,e){let i=vn(t),r=[i[ft.DayPeriodsFormat],i[ft.DayPeriodsStandalone]],o=ei(r,n);return ei(o,e)}function vD(t,n,e){let i=vn(t),r=[i[ft.DaysFormat],i[ft.DaysStandalone]],o=ei(r,n);return ei(o,e)}function bD(t,n,e){let i=vn(t),r=[i[ft.MonthsFormat],i[ft.MonthsStandalone]],o=ei(r,n);return ei(o,e)}function yD(t,n){let i=vn(t)[ft.Eras];return ei(i,n)}function rl(t,n){let e=vn(t);return ei(e[ft.DateFormat],n)}function ol(t,n){let e=vn(t);return ei(e[ft.TimeFormat],n)}function sl(t,n){let i=vn(t)[ft.DateTimeFormat];return ei(i,n)}function Wi(t,n){let e=vn(t),i=e[ft.NumberSymbols][n];if(typeof i>"u"){if(n===Mn.CurrencyDecimal)return e[ft.NumberSymbols][Mn.Decimal];if(n===Mn.CurrencyGroup)return e[ft.NumberSymbols][Mn.Group]}return i}function xD(t,n){return vn(t)[ft.NumberFormats][n]}function QO(t){return vn(t)[ft.Currencies]}function CD(t){if(!t[ft.ExtraData])throw new R(2303,!1)}function wD(t){let n=vn(t);return CD(n),(n[ft.ExtraData][2]||[]).map(i=>typeof i=="string"?gv(i):[gv(i[0]),gv(i[1])])}function DD(t,n,e){let i=vn(t);CD(i);let r=[i[ft.ExtraData][0],i[ft.ExtraData][1]],o=ei(r,n)||[];return ei(o,e)||[]}function ei(t,n){for(let e=n;e>-1;e--)if(typeof t[e]<"u")return t[e];throw new R(2304,!1)}function gv(t){let[n,e]=t.split(":");return{hours:+n,minutes:+e}}function ED(t,n,e="en"){let i=QO(e)[t]||pD[t]||[],r=i[1];return n==="narrow"&&typeof r=="string"?r:i[0]||t}var XO=2;function SD(t){let n,e=pD[t];return e&&(n=e[2]),typeof n=="number"?n:XO}var KO=/^(\d{4,})-?(\d\d)-?(\d\d)(?:T(\d\d)(?::?(\d\d)(?::?(\d\d)(?:\.(\d+))?)?)?(Z|([+-])(\d\d):?(\d\d))?)?$/,Em={},JO=/((?:[^BEGHLMOSWYZabcdhmswyz']+)|(?:'(?:[^']|'')*')|(?:G{1,5}|y{1,4}|Y{1,4}|M{1,5}|L{1,5}|w{1,2}|W{1}|d{1,2}|E{1,6}|c{1,6}|a{1,5}|b{1,5}|B{1,5}|h{1,2}|H{1,2}|m{1,2}|s{1,2}|S{1,3}|z{1,4}|Z{1,5}|O{1,4}))([\s\S]*)/,eN=256;function ID(t,n,e,i){let r=dN(t);tN(n),n=_r(e,n)||n;let s=[],a;for(;n;)if(a=JO.exec(n),a){s=s.concat(a.slice(1));let u=s.pop();if(!u)break;n=u}else{s.push(n);break}let c=r.getTimezoneOffset();i&&(c=MD(i,c),r=lN(r,i));let l="";return s.forEach(u=>{let m=aN(u);l+=m?m(r,e,c):u==="''"?"'":u.replace(/(^'|'$)/g,"").replace(/''/g,"'")}),l}function tN(t){if(t.length>eN)throw new R(2300,!1)}function Tm(t,n,e){let i=new Date(0);return i.setFullYear(t,n,e),i.setHours(0,0,0),i}function _r(t,n){let e=gD(t);if(Em[e]??={},Em[e][n])return Em[e][n];let i="";switch(n){case"shortDate":i=rl(t,kn.Short);break;case"mediumDate":i=rl(t,kn.Medium);break;case"longDate":i=rl(t,kn.Long);break;case"fullDate":i=rl(t,kn.Full);break;case"shortTime":i=ol(t,kn.Short);break;case"mediumTime":i=ol(t,kn.Medium);break;case"longTime":i=ol(t,kn.Long);break;case"fullTime":i=ol(t,kn.Full);break;case"short":let r=_r(t,"shortTime"),o=_r(t,"shortDate");i=Sm(sl(t,kn.Short),[r,o]);break;case"medium":let s=_r(t,"mediumTime"),a=_r(t,"mediumDate");i=Sm(sl(t,kn.Medium),[s,a]);break;case"long":let c=_r(t,"longTime"),l=_r(t,"longDate");i=Sm(sl(t,kn.Long),[c,l]);break;case"full":let u=_r(t,"fullTime"),m=_r(t,"fullDate");i=Sm(sl(t,kn.Full),[u,m]);break}return i&&(Em[e][n]=i),i}function Sm(t,n){return n&&(t=t.replace(/\{([^}]+)}/g,function(e,i){return n!=null&&i in n?n[i]:e})),t}function bi(t,n,e="-",i,r){let o="";(t<0||r&&t<=0)&&(r?t=-t+1:(t=-t,o=e));let s=String(t);for(;s.length<n;)s="0"+s;return i&&(s=s.slice(s.length-n)),o+s}function nN(t,n){return bi(t,3).substring(0,n)}function Nt(t,n,e=0,i=!1,r=!1){return function(o,s){let a=iN(t,o);if((e>0||a>-e)&&(a+=e),t===3)a===0&&e===-12&&(a=12);else if(t===6)return nN(a,n);let c=Wi(s,Mn.MinusSign);return bi(a,n,c,i,r)}}function iN(t,n){switch(t){case 0:return n.getFullYear();case 1:return n.getMonth();case 2:return n.getDate();case 3:return n.getHours();case 4:return n.getMinutes();case 5:return n.getSeconds();case 6:return n.getMilliseconds();case 7:return n.getDay();default:throw new R(2301,!1)}}function it(t,n,e=cn.Format,i=!1){return function(r,o){return rN(r,o,t,n,e,i)}}function rN(t,n,e,i,r,o){switch(e){case 2:return bD(n,r,i)[t.getMonth()];case 1:return vD(n,r,i)[t.getDay()];case 0:let s=t.getHours(),a=t.getMinutes();if(o){let l=wD(n),u=DD(n,r,i),m=l.findIndex(g=>{if(Array.isArray(g)){let[v,x]=g,F=s>=v.hours&&a>=v.minutes,j=s<x.hours||s===x.hours&&a<x.minutes;if(v.hours<x.hours){if(F&&j)return!0}else if(F||j)return!0}else if(g.hours===s&&g.minutes===a)return!0;return!1});if(m!==-1)return u[m]}return _D(n,r,i)[s<12?0:1];case 3:return yD(n,i)[t.getFullYear()<=0?0:1];default:let c=e;throw new R(2302,!1)}}function Im(t){return function(n,e,i){let r=-1*i,o=Wi(e,Mn.MinusSign),s=r>0?Math.floor(r/60):Math.ceil(r/60);switch(t){case 0:return(r>=0?"+":"")+bi(s,2,o)+bi(Math.abs(r%60),2,o);case 1:return"GMT"+(r>=0?"+":"")+bi(s,1,o);case 2:return"GMT"+(r>=0?"+":"")+bi(s,2,o)+":"+bi(Math.abs(r%60),2,o);case 3:return i===0?"Z":(r>=0?"+":"")+bi(s,2,o)+":"+bi(Math.abs(r%60),2,o);default:throw new R(2310,!1)}}}var oN=0,Mm=4;function sN(t){let n=Tm(t,oN,1).getDay();return Tm(t,0,1+(n<=Mm?Mm:Mm+7)-n)}function kD(t){let n=t.getDay(),e=n===0?-3:Mm-n;return Tm(t.getFullYear(),t.getMonth(),t.getDate()+e)}function _v(t,n=!1){return function(e,i){let r;if(n){let o=new Date(e.getFullYear(),e.getMonth(),1).getDay()-1,s=e.getDate();r=1+Math.floor((s+o)/7)}else{let o=kD(e),s=sN(o.getFullYear()),a=o.getTime()-s.getTime();r=1+Math.round(a/6048e5)}return bi(r,t,Wi(i,Mn.MinusSign))}}function km(t,n=!1){return function(e,i){let o=kD(e).getFullYear();return bi(o,t,Wi(i,Mn.MinusSign),n)}}var vv={};function aN(t){if(vv[t])return vv[t];let n;switch(t){case"G":case"GG":case"GGG":n=it(3,We.Abbreviated);break;case"GGGG":n=it(3,We.Wide);break;case"GGGGG":n=it(3,We.Narrow);break;case"y":n=Nt(0,1,0,!1,!0);break;case"yy":n=Nt(0,2,0,!0,!0);break;case"yyy":n=Nt(0,3,0,!1,!0);break;case"yyyy":n=Nt(0,4,0,!1,!0);break;case"Y":n=km(1);break;case"YY":n=km(2,!0);break;case"YYY":n=km(3);break;case"YYYY":n=km(4);break;case"M":case"L":n=Nt(1,1,1);break;case"MM":case"LL":n=Nt(1,2,1);break;case"MMM":n=it(2,We.Abbreviated);break;case"MMMM":n=it(2,We.Wide);break;case"MMMMM":n=it(2,We.Narrow);break;case"LLL":n=it(2,We.Abbreviated,cn.Standalone);break;case"LLLL":n=it(2,We.Wide,cn.Standalone);break;case"LLLLL":n=it(2,We.Narrow,cn.Standalone);break;case"w":n=_v(1);break;case"ww":n=_v(2);break;case"W":n=_v(1,!0);break;case"d":n=Nt(2,1);break;case"dd":n=Nt(2,2);break;case"c":case"cc":n=Nt(7,1);break;case"ccc":n=it(1,We.Abbreviated,cn.Standalone);break;case"cccc":n=it(1,We.Wide,cn.Standalone);break;case"ccccc":n=it(1,We.Narrow,cn.Standalone);break;case"cccccc":n=it(1,We.Short,cn.Standalone);break;case"E":case"EE":case"EEE":n=it(1,We.Abbreviated);break;case"EEEE":n=it(1,We.Wide);break;case"EEEEE":n=it(1,We.Narrow);break;case"EEEEEE":n=it(1,We.Short);break;case"a":case"aa":case"aaa":n=it(0,We.Abbreviated);break;case"aaaa":n=it(0,We.Wide);break;case"aaaaa":n=it(0,We.Narrow);break;case"b":case"bb":case"bbb":n=it(0,We.Abbreviated,cn.Standalone,!0);break;case"bbbb":n=it(0,We.Wide,cn.Standalone,!0);break;case"bbbbb":n=it(0,We.Narrow,cn.Standalone,!0);break;case"B":case"BB":case"BBB":n=it(0,We.Abbreviated,cn.Format,!0);break;case"BBBB":n=it(0,We.Wide,cn.Format,!0);break;case"BBBBB":n=it(0,We.Narrow,cn.Format,!0);break;case"h":n=Nt(3,1,-12);break;case"hh":n=Nt(3,2,-12);break;case"H":n=Nt(3,1);break;case"HH":n=Nt(3,2);break;case"m":n=Nt(4,1);break;case"mm":n=Nt(4,2);break;case"s":n=Nt(5,1);break;case"ss":n=Nt(5,2);break;case"S":n=Nt(6,1);break;case"SS":n=Nt(6,2);break;case"SSS":n=Nt(6,3);break;case"Z":case"ZZ":case"ZZZ":n=Im(0);break;case"ZZZZZ":n=Im(3);break;case"O":case"OO":case"OOO":case"z":case"zz":case"zzz":n=Im(1);break;case"OOOO":case"ZZZZ":case"zzzz":n=Im(2);break;default:return null}return vv[t]=n,n}function MD(t,n){t=t.replace(/:/g,"");let e=Date.parse("Jan 01, 1970 00:00:00 "+t)/6e4;return isNaN(e)?n:e}function cN(t,n){return t=new Date(t.getTime()),t.setMinutes(t.getMinutes()+n),t}function lN(t,n,e){let r=t.getTimezoneOffset(),o=MD(n,r);return cN(t,-1*(o-r))}function dN(t){if(mD(t))return t;if(typeof t=="number"&&!isNaN(t))return new Date(t);if(typeof t=="string"){if(t=t.trim(),/^(\d{4}(-\d{1,2}(-\d{1,2})?)?)$/.test(t)){let[r,o=1,s=1]=t.split("-").map(a=>+a);return Tm(r,o-1,s)}let e=parseFloat(t);if(!isNaN(t-e))return new Date(e);let i;if(i=t.match(KO))return uN(i)}let n=new Date(t);if(!mD(n))throw new R(2311,!1);return n}function uN(t){let n=new Date(0),e=0,i=0,r=t[8]?n.setUTCFullYear:n.setFullYear,o=t[8]?n.setUTCHours:n.setHours;t[9]&&(e=Number(t[9]+t[10]),i=Number(t[9]+t[11])),r.call(n,Number(t[1]),Number(t[2])-1,Number(t[3]));let s=Number(t[4]||0)-e,a=Number(t[5]||0)-i,c=Number(t[6]||0),l=Math.floor(parseFloat("0."+(t[7]||0))*1e3);return o.call(n,s,a,c,l),n}function mD(t){return t instanceof Date&&!isNaN(t.valueOf())}var mN=/^(\d+)?\.((\d+)(-(\d+))?)?$/,fD=22,Am=".",al="0",fN=";",hN=",",bv="#",hD="\xA4";function pN(t,n,e,i,r,o,s=!1){let a="",c=!1;if(!isFinite(t))a=Wi(e,Mn.Infinity);else{let l=vN(t);s&&(l=_N(l));let u=n.minInt,m=n.minFrac,g=n.maxFrac;if(o){let be=o.match(mN);if(be===null)throw new R(2306,!1);let Dt=be[1],pt=be[3],Ar=be[5];Dt!=null&&(u=yv(Dt)),pt!=null&&(m=yv(pt)),Ar!=null?g=yv(Ar):pt!=null&&m>g&&(g=m);let ir=100;if(u>ir||m>ir||g>ir)throw new R(2306,!1)}bN(l,m,g);let v=l.digits,x=l.integerLen,F=l.exponent,j=[];for(c=v.every(be=>!be);x<u;x++)v.unshift(0);for(;x<0;x++)v.unshift(0);x>0?j=v.splice(x,v.length):(j=v,v=[0]);let Y=[];for(v.length>=n.lgSize&&Y.unshift(v.splice(-n.lgSize,v.length).join(""));v.length>n.gSize;)Y.unshift(v.splice(-n.gSize,v.length).join(""));v.length&&Y.unshift(v.join("")),a=Y.join(Wi(e,i)),j.length&&(a+=Wi(e,r)+j.join("")),F&&(a+=Wi(e,Mn.Exponential)+"+"+F)}return t<0&&!c?a=n.negPre+a+n.negSuf:a=n.posPre+a+n.posSuf,a}function TD(t,n,e,i,r){let o=xD(n,xv.Currency),s=gN(o,Wi(n,Mn.MinusSign));return s.minFrac=SD(i),s.maxFrac=s.minFrac,pN(t,s,n,Mn.CurrencyGroup,Mn.CurrencyDecimal,r).replace(hD,e).replace(hD,"").trim()}function gN(t,n="-"){let e={minInt:1,minFrac:0,maxFrac:0,posPre:"",posSuf:"",negPre:"",negSuf:"",gSize:0,lgSize:0},i=t.split(fN),r=i[0],o=i[1],s=r.indexOf(Am)!==-1?r.split(Am):[r.substring(0,r.lastIndexOf(al)+1),r.substring(r.lastIndexOf(al)+1)],a=s[0],c=s[1]||"";e.posPre=a.substring(0,a.indexOf(bv));for(let u=0;u<c.length;u++){let m=c.charAt(u);m===al?e.minFrac=e.maxFrac=u+1:m===bv?e.maxFrac=u+1:e.posSuf+=m}let l=a.split(hN);if(e.gSize=l[1]?l[1].length:0,e.lgSize=l[2]||l[1]?(l[2]||l[1]).length:0,o){let u=r.length-e.posPre.length-e.posSuf.length,m=o.indexOf(bv);e.negPre=o.substring(0,m).replace(/'/g,""),e.negSuf=o.slice(m+u).replace(/'/g,"")}else e.negPre=n+e.posPre,e.negSuf=e.posSuf;return e}function _N(t){if(t.digits[0]===0)return t;let n=t.digits.length-t.integerLen;return t.exponent?t.exponent+=2:(n===0?t.digits.push(0,0):n===1&&t.digits.push(0),t.integerLen+=2),t}function vN(t){let n=Math.abs(t)+"",e=0,i,r,o,s,a;for((r=n.indexOf(Am))>-1&&(n=n.replace(Am,"")),(o=n.search(/e/i))>0?(r<0&&(r=o),r+=+n.slice(o+1),n=n.substring(0,o)):r<0&&(r=n.length),o=0;n.charAt(o)===al;o++);if(o===(a=n.length))i=[0],r=1;else{for(a--;n.charAt(a)===al;)a--;for(r-=o,i=[],s=0;o<=a;o++,s++)i[s]=Number(n.charAt(o))}return r>fD&&(i=i.splice(0,fD-1),e=r-1,r=1),{digits:i,exponent:e,integerLen:r}}function bN(t,n,e){if(n>e)throw new R(2307,!1);let i=t.digits,r=i.length-t.integerLen,o=Math.min(Math.max(n,r),e),s=o+t.integerLen,a=i[s];if(s>0){i.splice(Math.max(t.integerLen,s));for(let m=s;m<i.length;m++)i[m]=0}else{r=Math.max(0,r),t.integerLen=1,i.length=Math.max(1,s=o+1),i[0]=0;for(let m=1;m<s;m++)i[m]=0}if(a>=5)if(s-1<0){for(let m=0;m>s;m--)i.unshift(0),t.integerLen++;i.unshift(1),t.integerLen++}else i[s-1]++;for(;r<Math.max(0,o);r++)i.push(0);let c=o!==0,l=n+t.integerLen,u=i.reduceRight(function(m,g,v,x){return g=g+m,x[v]=g<10?g:g-10,c&&(x[v]===0&&v>=l?x.pop():c=!1),g>=10?1:0},0);u&&(i.unshift(u),t.integerLen++)}function yv(t){let n=parseInt(t);if(isNaN(n))throw new R(2305,!1);return n}var qi=(()=>{class t{_viewContainerRef;_viewRef=null;ngTemplateOutletContext=null;ngTemplateOutlet=null;ngTemplateOutletInjector=null;injector=d(X);constructor(e){this._viewContainerRef=e}ngOnChanges(e){if(this._shouldRecreateView(e)){let i=this._viewContainerRef;if(this._viewRef&&i.remove(i.indexOf(this._viewRef)),!this.ngTemplateOutlet){this._viewRef=null;return}let r=this._createContextForwardProxy();this._viewRef=i.createEmbeddedView(this.ngTemplateOutlet,r,{injector:this._getInjector()})}}_getInjector(){return this.ngTemplateOutletInjector==="outlet"?this.injector:this.ngTemplateOutletInjector??void 0}_shouldRecreateView(e){return!!e.ngTemplateOutlet||!!e.ngTemplateOutletInjector}_createContextForwardProxy(){return new Proxy({},{set:(e,i,r)=>this.ngTemplateOutletContext?Reflect.set(this.ngTemplateOutletContext,i,r):!1,get:(e,i,r)=>{if(this.ngTemplateOutletContext)return Reflect.get(this.ngTemplateOutletContext,i,r)}})}static \u0275fac=function(i){return new(i||t)(ee(Vt))};static \u0275dir=A({type:t,selectors:[["","ngTemplateOutlet",""]],inputs:{ngTemplateOutletContext:"ngTemplateOutletContext",ngTemplateOutlet:"ngTemplateOutlet",ngTemplateOutletInjector:"ngTemplateOutletInjector"},features:[ze]})}return t})();function AD(t,n){return new R(2100,!1)}var yN="mediumDate",RD=new y(""),OD=new y(""),rs=(()=>{class t{locale;defaultTimezone;defaultOptions;constructor(e,i,r){this.locale=e,this.defaultTimezone=i,this.defaultOptions=r}transform(e,i,r,o){if(e==null||e===""||e!==e)return null;try{let s=i??this.defaultOptions?.dateFormat??yN,a=r??this.defaultOptions?.timezone??this.defaultTimezone??void 0;return ID(e,s,o||this.locale,a)}catch(s){throw AD(t,s.message)}}static \u0275fac=function(i){return new(i||t)(ee(oa,16),ee(RD,24),ee(OD,24))};static \u0275pipe=Qr({name:"date",type:t,pure:!0})}return t})();var Ut=(()=>{class t{_locale;_defaultCurrencyCode;constructor(e,i="USD"){this._locale=e,this._defaultCurrencyCode=i}transform(e,i=this._defaultCurrencyCode,r="symbol",o,s){if(!xN(e))return null;s||=this._locale,typeof r=="boolean"&&(r=r?"symbol":"code");let a=i||this._defaultCurrencyCode;r!=="code"&&(r==="symbol"||r==="symbol-narrow"?a=ED(a,r==="symbol"?"wide":"narrow",s):a=r);try{let c=CN(e);return TD(c,s,a,i,o)}catch(c){throw AD(t,c.message)}}static \u0275fac=function(i){return new(i||t)(ee(oa,16),ee(dv,16))};static \u0275pipe=Qr({name:"currency",type:t,pure:!0})}return t})();function xN(t){return!(t==null||t===""||t!==t)}function CN(t){if(typeof t=="string"&&!isNaN(Number(t)-parseFloat(t)))return Number(t);if(typeof t!="number")throw new R(2309,!1);return t}function cl(t,n){n=encodeURIComponent(n);for(let e of t.split(";")){let i=e.indexOf("="),[r,o]=i==-1?[e,""]:[e.slice(0,i),e.slice(i+1)];if(r.trim()===n)return decodeURIComponent(o)}return null}var os=class{};var Cv="browser";function ND(t){return t===Cv}var ll=class{_doc;constructor(n){this._doc=n}manager},Rm=(()=>{class t extends ll{constructor(e){super(e)}supports(e){return!0}addEventListener(e,i,r,o){return e.addEventListener(i,r,o),()=>this.removeEventListener(e,i,r,o)}removeEventListener(e,i,r,o){return e.removeEventListener(i,r,o)}static \u0275fac=function(i){return new(i||t)(J(K))};static \u0275prov=C({token:t,factory:t.\u0275fac})}return t})(),Pm=new y(""),Sv=(()=>{class t{_zone;_plugins;_eventNameToPlugin=new Map;constructor(e,i){this._zone=i,e.forEach(s=>{s.manager=this});let r=e.filter(s=>!(s instanceof Rm));this._plugins=r.slice().reverse();let o=e.find(s=>s instanceof Rm);o&&this._plugins.push(o)}addEventListener(e,i,r,o){return this._findPluginFor(i).addEventListener(e,i,r,o)}getZone(){return this._zone}_findPluginFor(e){let i=this._eventNameToPlugin.get(e);if(i)return i;if(i=this._plugins.find(o=>o.supports(e)),!i)throw new R(5101,!1);return this._eventNameToPlugin.set(e,i),i}static \u0275fac=function(i){return new(i||t)(J(Pm),J(H))};static \u0275prov=C({token:t,factory:t.\u0275fac})}return t})(),wv="ng-app-id";function PD(t){for(let n of t)n.remove()}function FD(t,n){let e=n.createElement("style");return e.textContent=t,e}function SN(t,n,e,i){let r=t.head?.querySelectorAll(`style[${wv}="${n}"],link[${wv}="${n}"]`);if(r)for(let o of r)o.removeAttribute(wv),o instanceof HTMLLinkElement?i.set(o.href.slice(o.href.lastIndexOf("/")+1),{usage:0,elements:[o]}):o.textContent&&e.set(o.textContent,{usage:0,elements:[o]})}function Ev(t,n){let e=n.createElement("link");return e.setAttribute("rel","stylesheet"),e.setAttribute("href",t),e}var Iv=(()=>{class t{doc;appId;nonce;inline=new Map;external=new Map;hosts=new Set;constructor(e,i,r,o={}){this.doc=e,this.appId=i,this.nonce=r,SN(e,i,this.inline,this.external),this.hosts.add(e.head)}addStyles(e,i){for(let r of e)this.addUsage(r,this.inline,FD);i?.forEach(r=>this.addUsage(r,this.external,Ev))}removeStyles(e,i){for(let r of e)this.removeUsage(r,this.inline);i?.forEach(r=>this.removeUsage(r,this.external))}addUsage(e,i,r){let o=i.get(e);o?o.usage++:i.set(e,{usage:1,elements:[...this.hosts].map(s=>this.addElement(s,r(e,this.doc)))})}removeUsage(e,i){let r=i.get(e);r&&(r.usage--,r.usage<=0&&(PD(r.elements),i.delete(e)))}ngOnDestroy(){for(let[,{elements:e}]of[...this.inline,...this.external])PD(e);this.hosts.clear()}addHost(e){this.hosts.add(e);for(let[i,{elements:r}]of this.inline)r.push(this.addElement(e,FD(i,this.doc)));for(let[i,{elements:r}]of this.external)r.push(this.addElement(e,Ev(i,this.doc)))}removeHost(e){this.hosts.delete(e)}addElement(e,i){return this.nonce&&i.setAttribute("nonce",this.nonce),e.appendChild(i)}static \u0275fac=function(i){return new(i||t)(J(K),J(Yr),J(ts,8),J(es))};static \u0275prov=C({token:t,factory:t.\u0275fac})}return t})(),Dv={svg:"http://www.w3.org/2000/svg",xhtml:"http://www.w3.org/1999/xhtml",xlink:"http://www.w3.org/1999/xlink",xml:"http://www.w3.org/XML/1998/namespace",xmlns:"http://www.w3.org/2000/xmlns/",math:"http://www.w3.org/1998/Math/MathML"},kv=/%COMP%/g;var jD="%COMP%",IN=`_nghost-${jD}`,kN=`_ngcontent-${jD}`,MN=!0,TN=new y("",{factory:()=>MN});function AN(t){return kN.replace(kv,t)}function RN(t){return IN.replace(kv,t)}function BD(t,n){return n.map(e=>e.replace(kv,t))}var Mv=(()=>{class t{eventManager;sharedStylesHost;appId;removeStylesOnCompDestroy;doc;ngZone;nonce;tracingService;rendererByCompId=new Map;defaultRenderer;constructor(e,i,r,o,s,a,c=null,l=null){this.eventManager=e,this.sharedStylesHost=i,this.appId=r,this.removeStylesOnCompDestroy=o,this.doc=s,this.ngZone=a,this.nonce=c,this.tracingService=l,this.defaultRenderer=new dl(e,s,a,this.tracingService)}createRenderer(e,i){if(!e||!i)return this.defaultRenderer;let r=this.getOrCreateRenderer(e,i);return r instanceof Nm?r.applyToHost(e):r instanceof ul&&r.applyStyles(),r}getOrCreateRenderer(e,i){let r=this.rendererByCompId,o=r.get(i.id);if(!o){let s=this.doc,a=this.ngZone,c=this.eventManager,l=this.sharedStylesHost,u=this.removeStylesOnCompDestroy,m=this.tracingService;switch(i.encapsulation){case pi.Emulated:o=new Nm(c,l,i,this.appId,u,s,a,m);break;case pi.ShadowDom:return new Om(c,e,i,s,a,this.nonce,m,l);case pi.ExperimentalIsolatedShadowDom:return new Om(c,e,i,s,a,this.nonce,m);default:o=new ul(c,l,i,u,s,a,m);break}r.set(i.id,o)}return o}ngOnDestroy(){this.rendererByCompId.clear()}componentReplaced(e){this.rendererByCompId.delete(e)}static \u0275fac=function(i){return new(i||t)(J(Sv),J(Iv),J(Yr),J(TN),J(K),J(H),J(ts),J(_i,8))};static \u0275prov=C({token:t,factory:t.\u0275fac})}return t})(),dl=class{eventManager;doc;ngZone;tracingService;data=Object.create(null);throwOnSyntheticProps=!0;constructor(n,e,i,r){this.eventManager=n,this.doc=e,this.ngZone=i,this.tracingService=r}destroy(){}destroyNode=null;createElement(n,e){return e?this.doc.createElementNS(Dv[e]||e,n):this.doc.createElement(n)}createComment(n){return this.doc.createComment(n)}createText(n){return this.doc.createTextNode(n)}appendChild(n,e){(LD(n)?n.content:n).appendChild(e)}insertBefore(n,e,i){n&&(LD(n)?n.content:n).insertBefore(e,i)}removeChild(n,e){e.remove()}selectRootElement(n,e){let i=typeof n=="string"?this.doc.querySelector(n):n;if(!i)throw new R(-5104,!1);return e||(i.textContent=""),i}parentNode(n){return n.parentNode}nextSibling(n){return n.nextSibling}setAttribute(n,e,i,r){if(r){e=r+":"+e;let o=Dv[r];o?n.setAttributeNS(o,e,i):n.setAttribute(e,i)}else n.setAttribute(e,i)}removeAttribute(n,e,i){if(i){let r=Dv[i];r?n.removeAttributeNS(r,e):n.removeAttribute(`${i}:${e}`)}else n.removeAttribute(e)}addClass(n,e){n.classList.add(e)}removeClass(n,e){n.classList.remove(e)}setStyle(n,e,i,r){r&(Ui.DashCase|Ui.Important)?n.style.setProperty(e,i,r&Ui.Important?"important":""):n.style[e]=i}removeStyle(n,e,i){i&Ui.DashCase?n.style.removeProperty(e):n.style[e]=""}setProperty(n,e,i){n!=null&&(n[e]=i)}setValue(n,e){n.nodeValue=e}listen(n,e,i,r){if(typeof n=="string"&&(n=Jn().getGlobalEventTarget(this.doc,n),!n))throw new R(5102,!1);let o=this.decoratePreventDefault(i);return this.tracingService?.wrapEventListener&&(o=this.tracingService.wrapEventListener(n,e,o)),this.eventManager.addEventListener(n,e,o,r)}decoratePreventDefault(n){return e=>{if(e==="__ngUnwrap__")return n;n(e)===!1&&e.preventDefault()}}};function LD(t){return t.tagName==="TEMPLATE"&&t.content!==void 0}var Om=class extends dl{hostEl;sharedStylesHost;shadowRoot;constructor(n,e,i,r,o,s,a,c){super(n,r,o,a),this.hostEl=e,this.sharedStylesHost=c,this.shadowRoot=e.attachShadow({mode:"open"}),this.sharedStylesHost&&this.sharedStylesHost.addHost(this.shadowRoot);let l=i.styles;l=BD(i.id,l);for(let m of l){let g=document.createElement("style");s&&g.setAttribute("nonce",s),g.textContent=m,this.shadowRoot.appendChild(g)}let u=i.getExternalStyles?.();if(u)for(let m of u){let g=Ev(m,r);s&&g.setAttribute("nonce",s),this.shadowRoot.appendChild(g)}}nodeOrShadowRoot(n){return n===this.hostEl?this.shadowRoot:n}appendChild(n,e){return super.appendChild(this.nodeOrShadowRoot(n),e)}insertBefore(n,e,i){return super.insertBefore(this.nodeOrShadowRoot(n),e,i)}removeChild(n,e){return super.removeChild(null,e)}parentNode(n){return this.nodeOrShadowRoot(super.parentNode(this.nodeOrShadowRoot(n)))}destroy(){this.sharedStylesHost&&this.sharedStylesHost.removeHost(this.shadowRoot)}},ul=class extends dl{sharedStylesHost;removeStylesOnCompDestroy;styles;styleUrls;constructor(n,e,i,r,o,s,a,c){super(n,o,s,a),this.sharedStylesHost=e,this.removeStylesOnCompDestroy=r;let l=i.styles;this.styles=c?BD(c,l):l,this.styleUrls=i.getExternalStyles?.(c)}applyStyles(){this.sharedStylesHost.addStyles(this.styles,this.styleUrls)}destroy(){this.removeStylesOnCompDestroy&&Xo.size===0&&this.sharedStylesHost.removeStyles(this.styles,this.styleUrls)}},Nm=class extends ul{contentAttr;hostAttr;constructor(n,e,i,r,o,s,a,c){let l=r+"-"+i.id;super(n,e,i,o,s,a,c,l),this.contentAttr=AN(l),this.hostAttr=RN(l)}applyToHost(n){this.applyStyles(),this.setAttribute(n,this.hostAttr,"")}createElement(n,e){let i=super.createElement(n,e);return super.setAttribute(i,this.contentAttr,""),i}};var Fm=class t extends il{supportsDOMEvents=!0;static makeCurrent(){pv(new t)}onAndCancel(n,e,i,r){return n.addEventListener(e,i,r),()=>{n.removeEventListener(e,i,r)}}dispatchEvent(n,e){n.dispatchEvent(e)}remove(n){n.remove()}createElement(n,e){return e=e||this.getDefaultDocument(),e.createElement(n)}createHtmlDocument(){return document.implementation.createHTMLDocument("fakeTitle")}getDefaultDocument(){return document}isElementNode(n){return n.nodeType===Node.ELEMENT_NODE}isShadowRoot(n){return n instanceof DocumentFragment}getGlobalEventTarget(n,e){return e==="window"?window:e==="document"?n:e==="body"?n.body:null}getBaseHref(n){let e=ON();return e==null?null:NN(e)}resetBaseElement(){ml=null}getUserAgent(){return window.navigator.userAgent}getCookie(n){return cl(document.cookie,n)}},ml=null;function ON(){return ml=ml||document.head.querySelector("base"),ml?ml.getAttribute("href"):null}function NN(t){return new URL(t,document.baseURI).pathname}var PN=(()=>{class t{build(){return new XMLHttpRequest}static \u0275fac=function(i){return new(i||t)};static \u0275prov=C({token:t,factory:t.\u0275fac})}return t})(),VD=["alt","control","meta","shift"],FN={"\b":"Backspace","	":"Tab","\x7F":"Delete","\x1B":"Escape",Del:"Delete",Esc:"Escape",Left:"ArrowLeft",Right:"ArrowRight",Up:"ArrowUp",Down:"ArrowDown",Menu:"ContextMenu",Scroll:"ScrollLock",Win:"OS"},LN={alt:t=>t.altKey,control:t=>t.ctrlKey,meta:t=>t.metaKey,shift:t=>t.shiftKey},HD=(()=>{class t extends ll{constructor(e){super(e)}supports(e){return t.parseEventName(e)!=null}addEventListener(e,i,r,o){let s=t.parseEventName(i),a=t.eventCallback(s.fullKey,r,this.manager.getZone());return this.manager.getZone().runOutsideAngular(()=>Jn().onAndCancel(e,s.domEventName,a,o))}static parseEventName(e){let i=e.toLowerCase().split("."),r=i.shift();if(i.length===0||!(r==="keydown"||r==="keyup"))return null;let o=t._normalizeKey(i.pop()),s="",a=i.indexOf("code");if(a>-1&&(i.splice(a,1),s="code."),VD.forEach(l=>{let u=i.indexOf(l);u>-1&&(i.splice(u,1),s+=l+".")}),s+=o,i.length!=0||o.length===0)return null;let c={};return c.domEventName=r,c.fullKey=s,c}static matchEventFullKeyCode(e,i){let r=FN[e.key]||e.key,o="";return i.indexOf("code.")>-1&&(r=e.code,o="code."),r==null||!r?!1:(r=r.toLowerCase(),r===" "?r="space":r==="."&&(r="dot"),VD.forEach(s=>{if(s!==r){let a=LN[s];a(e)&&(o+=s+".")}}),o+=r,o===i)}static eventCallback(e,i,r){return o=>{t.matchEventFullKeyCode(o,e)&&r.runGuarded(()=>i(o))}}static _normalizeKey(e){return e==="esc"?"escape":e}static \u0275fac=function(i){return new(i||t)(J(K))};static \u0275prov=C({token:t,factory:t.\u0275fac})}return t})();async function Tv(t,n,e){let i=b({rootComponent:t},jN(n,e));return rD(i)}function jN(t,n){return{platformRef:n?.platformRef,appProviders:[...zN,...t?.providers??[]],platformProviders:UN}}function BN(){Fm.makeCurrent()}function VN(){return new rn}function HN(){return x_(document),document}var UN=[{provide:es,useValue:Cv},{provide:tm,useValue:BN,multi:!0},{provide:K,useFactory:HN}];var zN=[{provide:Ec,useValue:"root"},{provide:rn,useFactory:VN},{provide:Pm,useClass:Rm,multi:!0},{provide:Pm,useClass:HD,multi:!0},Mv,Iv,Sv,{provide:Ot,useExisting:Mv},{provide:os,useClass:PN},[]];var to=class t{headers;normalizedNames=new Map;lazyInit;lazyUpdate=null;constructor(n){n?typeof n=="string"?this.lazyInit=()=>{this.headers=new Map,n.split(`
`).forEach(e=>{let i=e.indexOf(":");if(i>0){let r=e.slice(0,i),o=e.slice(i+1).trim();this.addHeaderEntry(r,o)}})}:typeof Headers<"u"&&n instanceof Headers?(this.headers=new Map,n.forEach((e,i)=>{this.addHeaderEntry(i,e)})):this.lazyInit=()=>{this.headers=new Map,Object.entries(n).forEach(([e,i])=>{this.setHeaderEntries(e,i)})}:this.headers=new Map}has(n){return this.init(),this.headers.has(n.toLowerCase())}get(n){this.init();let e=this.headers.get(n.toLowerCase());return e&&e.length>0?e[0]:null}keys(){return this.init(),Array.from(this.normalizedNames.values())}getAll(n){return this.init(),this.headers.get(n.toLowerCase())||null}append(n,e){return this.clone({name:n,value:e,op:"a"})}set(n,e){return this.clone({name:n,value:e,op:"s"})}delete(n,e){return this.clone({name:n,value:e,op:"d"})}maybeSetNormalizedName(n,e){this.normalizedNames.has(e)||this.normalizedNames.set(e,n)}init(){this.lazyInit&&(this.lazyInit instanceof t?this.copyFrom(this.lazyInit):this.lazyInit(),this.lazyInit=null,this.lazyUpdate&&(this.lazyUpdate.forEach(n=>this.applyUpdate(n)),this.lazyUpdate=null))}copyFrom(n){n.init(),Array.from(n.headers.keys()).forEach(e=>{this.headers.set(e,n.headers.get(e)),this.normalizedNames.set(e,n.normalizedNames.get(e))})}clone(n){let e=new t;return e.lazyInit=this.lazyInit&&this.lazyInit instanceof t?this.lazyInit:this,e.lazyUpdate=(this.lazyUpdate||[]).concat([n]),e}applyUpdate(n){let e=n.name.toLowerCase();switch(n.op){case"a":case"s":let i=n.value;if(typeof i=="string"&&(i=[i]),i.length===0)return;this.maybeSetNormalizedName(n.name,e);let r=(n.op==="a"?this.headers.get(e):void 0)||[];r.push(...i),this.headers.set(e,r);break;case"d":let o=n.value;if(!o)this.headers.delete(e),this.normalizedNames.delete(e);else{let s=this.headers.get(e);if(!s)return;s=s.filter(a=>o.indexOf(a)===-1),s.length===0?(this.headers.delete(e),this.normalizedNames.delete(e)):this.headers.set(e,s)}break}}addHeaderEntry(n,e){let i=n.toLowerCase();this.maybeSetNormalizedName(n,i),this.headers.has(i)?this.headers.get(i).push(e):this.headers.set(i,[e])}setHeaderEntries(n,e){let i=(Array.isArray(e)?e:[e]).map(o=>o.toString()),r=n.toLowerCase();this.headers.set(r,i),this.maybeSetNormalizedName(n,r)}forEach(n){this.init(),Array.from(this.normalizedNames.keys()).forEach(e=>n(this.normalizedNames.get(e),this.headers.get(e)))}};var jm=class{map=new Map;set(n,e){return this.map.set(n,e),this}get(n){return this.map.has(n)||this.map.set(n,n.defaultValue()),this.map.get(n)}delete(n){return this.map.delete(n),this}has(n){return this.map.has(n)}keys(){return this.map.keys()}},Bm=class{encodeKey(n){return UD(n)}encodeValue(n){return UD(n)}decodeKey(n){return decodeURIComponent(n)}decodeValue(n){return decodeURIComponent(n)}};function $N(t,n){let e=new Map;return t.length>0&&t.replace(/^\?/,"").split("&").forEach(r=>{let o=r.indexOf("="),[s,a]=o==-1?[n.decodeKey(r),""]:[n.decodeKey(r.slice(0,o)),n.decodeValue(r.slice(o+1))],c=e.get(s)||[];c.push(a),e.set(s,c)}),e}var GN=/%(\d[a-f0-9])/gi,WN={40:"@","3A":":",24:"$","2C":",","3B":";","3D":"=","3F":"?","2F":"/"};function UD(t){return encodeURIComponent(t).replace(GN,(n,e)=>WN[e]??n)}function Lm(t){return`${t}`}var jn=class t{map;encoder;updates=null;cloneFrom=null;constructor(n={}){if(this.encoder=n.encoder||new Bm,n.fromString){if(n.fromObject)throw new R(2805,!1);this.map=$N(n.fromString,this.encoder)}else n.fromObject?(this.map=new Map,Object.keys(n.fromObject).forEach(e=>{let i=n.fromObject[e],r=Array.isArray(i)?i.map(Lm):[Lm(i)];this.map.set(e,r)})):this.map=null}has(n){return this.init(),this.map.has(n)}get(n){this.init();let e=this.map.get(n);return e?e[0]:null}getAll(n){return this.init(),this.map.get(n)||null}keys(){return this.init(),Array.from(this.map.keys())}append(n,e){return this.clone({param:n,value:e,op:"a"})}appendAll(n){let e=[];return Object.keys(n).forEach(i=>{let r=n[i];Array.isArray(r)?r.forEach(o=>{e.push({param:i,value:o,op:"a"})}):e.push({param:i,value:r,op:"a"})}),this.clone(e)}set(n,e){return this.clone({param:n,value:e,op:"s"})}delete(n,e){return this.clone({param:n,value:e,op:"d"})}toString(){return this.init(),this.keys().map(n=>{let e=this.encoder.encodeKey(n);return this.map.get(n).map(i=>e+"="+this.encoder.encodeValue(i)).join("&")}).filter(n=>n!=="").join("&")}clone(n){let e=new t({encoder:this.encoder});return e.cloneFrom=this.cloneFrom||this,e.updates=(this.updates||[]).concat(n),e}init(){this.map===null&&(this.map=new Map),this.cloneFrom!==null&&(this.cloneFrom.init(),this.cloneFrom.keys().forEach(n=>this.map.set(n,this.cloneFrom.map.get(n))),this.updates.forEach(n=>{switch(n.op){case"a":case"s":let e=(n.op==="a"?this.map.get(n.param):void 0)||[];e.push(Lm(n.value)),this.map.set(n.param,e);break;case"d":if(n.value!==void 0){let i=this.map.get(n.param)||[],r=i.indexOf(Lm(n.value));r!==-1&&i.splice(r,1),i.length>0?this.map.set(n.param,i):this.map.delete(n.param)}else{this.map.delete(n.param);break}}}),this.cloneFrom=this.updates=null)}};function qN(t){switch(t){case"DELETE":case"GET":case"HEAD":case"OPTIONS":case"JSONP":return!1;default:return!0}}function zD(t){return typeof ArrayBuffer<"u"&&t instanceof ArrayBuffer}function $D(t){return typeof Blob<"u"&&t instanceof Blob}function GD(t){return typeof FormData<"u"&&t instanceof FormData}function YN(t){return typeof URLSearchParams<"u"&&t instanceof URLSearchParams}var WD="Content-Type",qD="Accept",YD="text/plain",ZD="application/json",ZN=`${ZD}, ${YD}, */*`,la=class t{url;body=null;headers;context;reportProgress=!1;withCredentials=!1;credentials;keepalive=!1;cache;priority;mode;redirect;referrer;integrity;referrerPolicy;responseType="json";method;params;urlWithParams;transferCache;timeout;constructor(n,e,i,r){this.url=e,this.method=n.toUpperCase();let o;if(qN(this.method)||r?(this.body=i!==void 0?i:null,o=r):o=i,o){if(this.reportProgress=!!o.reportProgress,this.withCredentials=!!o.withCredentials,this.keepalive=!!o.keepalive,o.responseType&&(this.responseType=o.responseType),o.headers&&(this.headers=o.headers),o.context&&(this.context=o.context),o.params&&(this.params=o.params),o.priority&&(this.priority=o.priority),o.cache&&(this.cache=o.cache),o.credentials&&(this.credentials=o.credentials),typeof o.timeout=="number"){if(o.timeout<1||!Number.isInteger(o.timeout))throw new R(2822,"");this.timeout=o.timeout}o.mode&&(this.mode=o.mode),o.redirect&&(this.redirect=o.redirect),o.integrity&&(this.integrity=o.integrity),o.referrer!==void 0&&(this.referrer=o.referrer),o.referrerPolicy&&(this.referrerPolicy=o.referrerPolicy),this.transferCache=o.transferCache}if(this.headers??=new to,this.context??=new jm,!this.params)this.params=new jn,this.urlWithParams=e;else{let s=this.params.toString();if(s.length===0)this.urlWithParams=e;else{let a=e.indexOf("?"),c=a===-1?"?":a<e.length-1?"&":"";this.urlWithParams=e+c+s}}}serializeBody(){return this.body===null?null:typeof this.body=="string"||zD(this.body)||$D(this.body)||GD(this.body)||YN(this.body)?this.body:this.body instanceof jn?this.body.toString():typeof this.body=="object"||typeof this.body=="boolean"||Array.isArray(this.body)?JSON.stringify(this.body):this.body.toString()}detectContentTypeHeader(){return this.body===null||GD(this.body)?null:$D(this.body)?this.body.type||null:zD(this.body)?null:typeof this.body=="string"?YD:this.body instanceof jn?"application/x-www-form-urlencoded;charset=UTF-8":typeof this.body=="object"||typeof this.body=="number"||typeof this.body=="boolean"?ZD:null}clone(n={}){let e=n.method||this.method,i=n.url||this.url,r=n.responseType||this.responseType,o=n.keepalive??this.keepalive,s=n.priority||this.priority,a=n.cache||this.cache,c=n.mode||this.mode,l=n.redirect||this.redirect,u=n.credentials||this.credentials,m=n.referrer??this.referrer,g=n.integrity||this.integrity,v=n.referrerPolicy||this.referrerPolicy,x=n.transferCache??this.transferCache,F=n.timeout??this.timeout,j=n.body!==void 0?n.body:this.body,Y=n.withCredentials??this.withCredentials,be=n.reportProgress??this.reportProgress,Dt=n.headers||this.headers,pt=n.params||this.params,Ar=n.context??this.context;return n.setHeaders!==void 0&&(Dt=Object.keys(n.setHeaders).reduce((ir,Co)=>ir.set(Co,n.setHeaders[Co]),Dt)),n.setParams&&(pt=Object.keys(n.setParams).reduce((ir,Co)=>ir.set(Co,n.setParams[Co]),pt)),new t(e,i,j,{params:pt,headers:Dt,context:Ar,reportProgress:be,responseType:r,withCredentials:Y,transferCache:x,keepalive:o,cache:a,priority:s,timeout:F,mode:c,redirect:l,credentials:u,referrer:m,integrity:g,referrerPolicy:v})}},ss=(function(t){return t[t.Sent=0]="Sent",t[t.UploadProgress=1]="UploadProgress",t[t.ResponseHeader=2]="ResponseHeader",t[t.DownloadProgress=3]="DownloadProgress",t[t.Response=4]="Response",t[t.User=5]="User",t})(ss||{}),ua=class{headers;status;statusText;url;ok;type;redirected;responseType;constructor(n,e=200,i="OK"){this.headers=n.headers||new to,this.status=n.status!==void 0?n.status:e,this.statusText=n.statusText||i,this.url=n.url||null,this.redirected=n.redirected,this.responseType=n.responseType,this.ok=this.status>=200&&this.status<300}},Vm=class t extends ua{constructor(n={}){super(n)}type=ss.ResponseHeader;clone(n={}){return new t({headers:n.headers||this.headers,status:n.status!==void 0?n.status:this.status,statusText:n.statusText||this.statusText,url:n.url||this.url||void 0})}},fl=class t extends ua{body;constructor(n={}){super(n),this.body=n.body!==void 0?n.body:null}type=ss.Response;clone(n={}){return new t({body:n.body!==void 0?n.body:this.body,headers:n.headers||this.headers,status:n.status!==void 0?n.status:this.status,statusText:n.statusText||this.statusText,url:n.url||this.url||void 0,redirected:n.redirected??this.redirected,responseType:n.responseType??this.responseType})}},da=class extends ua{name="HttpErrorResponse";message;error;ok=!1;constructor(n){super(n,0,"Unknown Error"),this.status>=200&&this.status<300?this.message=`Http failure during parsing for ${n.url||"(unknown url)"}`:this.message=`Http failure response for ${n.url||"(unknown url)"}: ${n.status} ${n.statusText}`,this.error=n.error||null}},QN=200,XN=204;var KN=new y("");var JN=/^\)\]\}',?\n/;var Rv=(()=>{class t{xhrFactory;tracingService=d(_i,{optional:!0});constructor(e){this.xhrFactory=e}maybePropagateTrace(e){return this.tracingService?.propagate?this.tracingService.propagate(e):e}handle(e){if(e.method==="JSONP")throw new R(-2800,!1);let i=this.xhrFactory;return W(null).pipe(gt(()=>new ce(o=>{let s=i.build();if(s.open(e.method,e.urlWithParams),e.withCredentials&&(s.withCredentials=!0),e.headers.forEach((j,Y)=>s.setRequestHeader(j,Y.join(","))),e.headers.has(qD)||s.setRequestHeader(qD,ZN),!e.headers.has(WD)){let j=e.detectContentTypeHeader();j!==null&&s.setRequestHeader(WD,j)}if(e.timeout&&(s.timeout=e.timeout),e.responseType){let j=e.responseType.toLowerCase();s.responseType=j!=="json"?j:"text"}let a=e.serializeBody(),c=null,l=()=>{if(c!==null)return c;let j=s.statusText||"OK",Y=new to(s.getAllResponseHeaders()),be=s.responseURL||e.url;return c=new Vm({headers:Y,status:s.status,statusText:j,url:be}),c},u=this.maybePropagateTrace(()=>{let{headers:j,status:Y,statusText:be,url:Dt}=l(),pt=null;Y!==XN&&(pt=typeof s.response>"u"?s.responseText:s.response),Y===0&&(Y=pt?QN:0);let Ar=Y>=200&&Y<300;if(e.responseType==="json"&&typeof pt=="string"){let ir=pt;pt=pt.replace(JN,"");try{pt=pt!==""?JSON.parse(pt):null}catch(Co){pt=ir,Ar&&(Ar=!1,pt={error:Co,text:pt})}}Ar?(o.next(new fl({body:pt,headers:j,status:Y,statusText:be,url:Dt||void 0})),o.complete()):o.error(new da({error:pt,headers:j,status:Y,statusText:be,url:Dt||void 0}))}),m=this.maybePropagateTrace(j=>{let{url:Y}=l(),be=new da({error:j,status:s.status||0,statusText:s.statusText||"Unknown Error",url:Y||void 0});o.error(be)}),g=m;e.timeout&&(g=this.maybePropagateTrace(j=>{let{url:Y}=l(),be=new da({error:new DOMException("Request timed out","TimeoutError"),status:s.status||0,statusText:s.statusText||"Request timeout",url:Y||void 0});o.error(be)}));let v=!1,x=this.maybePropagateTrace(j=>{v||(o.next(l()),v=!0);let Y={type:ss.DownloadProgress,loaded:j.loaded};j.lengthComputable&&(Y.total=j.total),e.responseType==="text"&&s.responseText&&(Y.partialText=s.responseText),o.next(Y)}),F=this.maybePropagateTrace(j=>{let Y={type:ss.UploadProgress,loaded:j.loaded};j.lengthComputable&&(Y.total=j.total),o.next(Y)});return s.addEventListener("load",u),s.addEventListener("error",m),s.addEventListener("timeout",g),s.addEventListener("abort",m),e.reportProgress&&(s.addEventListener("progress",x),a!==null&&s.upload&&s.upload.addEventListener("progress",F)),s.send(a),o.next({type:ss.Sent}),()=>{s.removeEventListener("error",m),s.removeEventListener("abort",m),s.removeEventListener("load",u),s.removeEventListener("timeout",g),e.reportProgress&&(s.removeEventListener("progress",x),a!==null&&s.upload&&s.upload.removeEventListener("progress",F)),s.readyState!==s.DONE&&s.abort()}})))}static \u0275fac=function(i){return new(i||t)(J(os))};static \u0275prov=C({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();function eP(t,n){return n(t)}function tP(t,n,e){return(i,r)=>$t(e,()=>n(i,o=>t(o,r)))}var Ov=new y("",{factory:()=>[]}),QD=new y(""),XD=new y("",{factory:()=>!0});var Nv=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275prov=C({token:t,factory:function(i){let r=null;return i?r=new(i||t):r=J(Rv),r},providedIn:"root"})}return t})();var Hm=(()=>{class t{backend;injector;chain=null;pendingTasks=d(Rc);contributeToStability=d(XD);constructor(e,i){this.backend=e,this.injector=i}handle(e){if(this.chain===null){let i=Array.from(new Set([...this.injector.get(Ov),...this.injector.get(QD,[])]));this.chain=i.reduceRight((r,o)=>tP(r,o,this.injector),eP)}if(this.contributeToStability){let i=this.pendingTasks.add();return this.chain(e,r=>this.backend.handle(r)).pipe(or(i))}else return this.chain(e,i=>this.backend.handle(i))}static \u0275fac=function(i){return new(i||t)(J(Nv),J(He))};static \u0275prov=C({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})(),Pv=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275prov=C({token:t,factory:function(i){let r=null;return i?r=new(i||t):r=J(Hm),r},providedIn:"root"})}return t})();function Av(t,n){return{body:n,headers:t.headers,context:t.context,observe:t.observe,params:t.params,reportProgress:t.reportProgress,responseType:t.responseType,withCredentials:t.withCredentials,credentials:t.credentials,transferCache:t.transferCache,timeout:t.timeout,keepalive:t.keepalive,priority:t.priority,cache:t.cache,mode:t.mode,redirect:t.redirect,integrity:t.integrity,referrer:t.referrer,referrerPolicy:t.referrerPolicy}}var Wt=(()=>{class t{handler;constructor(e){this.handler=e}request(e,i,r={}){let o;if(e instanceof la)o=e;else{let c;r.headers instanceof to?c=r.headers:c=new to(r.headers);let l;r.params&&(r.params instanceof jn?l=r.params:l=new jn({fromObject:r.params})),o=new la(e,i,r.body!==void 0?r.body:null,{headers:c,context:r.context,params:l,reportProgress:r.reportProgress,responseType:r.responseType||"json",withCredentials:r.withCredentials,transferCache:r.transferCache,keepalive:r.keepalive,priority:r.priority,cache:r.cache,mode:r.mode,redirect:r.redirect,credentials:r.credentials,referrer:r.referrer,referrerPolicy:r.referrerPolicy,integrity:r.integrity,timeout:r.timeout})}let s=W(o).pipe(No(c=>this.handler.handle(c)));if(e instanceof la||r.observe==="events")return s;let a=s.pipe(De(c=>c instanceof fl));switch(r.observe||"body"){case"body":switch(o.responseType){case"arraybuffer":return a.pipe(Q(c=>{if(c.body!==null&&!(c.body instanceof ArrayBuffer))throw new R(2806,!1);return c.body}));case"blob":return a.pipe(Q(c=>{if(c.body!==null&&!(c.body instanceof Blob))throw new R(2807,!1);return c.body}));case"text":return a.pipe(Q(c=>{if(c.body!==null&&typeof c.body!="string")throw new R(2808,!1);return c.body}));default:return a.pipe(Q(c=>c.body))}case"response":return a;default:throw new R(2809,!1)}}delete(e,i={}){return this.request("DELETE",e,i)}get(e,i={}){return this.request("GET",e,i)}head(e,i={}){return this.request("HEAD",e,i)}jsonp(e,i){return this.request("JSONP",e,{params:new jn().append(i,"JSONP_CALLBACK"),observe:"body",responseType:"json"})}options(e,i={}){return this.request("OPTIONS",e,i)}patch(e,i,r={}){return this.request("PATCH",e,Av(r,i))}post(e,i,r={}){return this.request("POST",e,Av(r,i))}put(e,i,r={}){return this.request("PUT",e,Av(r,i))}static \u0275fac=function(i){return new(i||t)(J(Pv))};static \u0275prov=C({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();var nP=new y("",{factory:()=>!0}),iP="XSRF-TOKEN",rP=new y("",{factory:()=>iP}),oP="X-XSRF-TOKEN",sP=new y("",{factory:()=>oP}),aP=(()=>{class t{cookieName=d(rP);doc=d(K);lastCookieString="";lastToken=null;parseCount=0;getToken(){let e=this.doc.cookie||"";return e!==this.lastCookieString&&(this.parseCount++,this.lastToken=cl(e,this.cookieName),this.lastCookieString=e),this.lastToken}static \u0275fac=function(i){return new(i||t)};static \u0275prov=C({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})(),KD=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275prov=C({token:t,factory:function(i){let r=null;return i?r=new(i||t):r=J(aP),r},providedIn:"root"})}return t})();function cP(t,n){if(!d(nP)||t.method==="GET"||t.method==="HEAD")return n(t);try{let r=d(aa).href,{origin:o}=new URL(r),{origin:s}=new URL(t.url,o);if(o!==s)return n(t)}catch{return n(t)}let e=d(KD).getToken(),i=d(sP);return e!=null&&!t.headers.has(i)&&(t=t.clone({headers:t.headers.set(i,e)})),n(t)}var Fv=(function(t){return t[t.Interceptors=0]="Interceptors",t[t.LegacyInterceptors=1]="LegacyInterceptors",t[t.CustomXsrfConfiguration=2]="CustomXsrfConfiguration",t[t.NoXsrfProtection=3]="NoXsrfProtection",t[t.JsonpSupport=4]="JsonpSupport",t[t.RequestsMadeViaParent=5]="RequestsMadeViaParent",t[t.Fetch=6]="Fetch",t})(Fv||{});function lP(t,n){return{\u0275kind:t,\u0275providers:n}}function Lv(...t){let n=[Wt,Hm,{provide:Pv,useExisting:Hm},{provide:Nv,useFactory:()=>d(KN,{optional:!0})??d(Rv)},{provide:Ov,useValue:cP,multi:!0}];for(let e of t)n.push(...e.\u0275providers);return Ni(n)}function jv(t){return lP(Fv.Interceptors,t.map(n=>({provide:Ov,useValue:n,multi:!0})))}var JD=(()=>{class t{_doc;constructor(e){this._doc=e}getTitle(){return this._doc.title}setTitle(e){this._doc.title=e||""}static \u0275fac=function(i){return new(i||t)(J(K))};static \u0275prov=C({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();var hl=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275prov=C({token:t,factory:function(i){let r=null;return i?r=new(i||t):r=J(uP),r},providedIn:"root"})}return t})(),uP=(()=>{class t extends hl{_doc;constructor(e){super(),this._doc=e}sanitize(e,i){if(i==null)return null;switch(e){case Bt.NONE:return i;case Bt.HTML:return pr(i,"HTML")?gi(i):k_(this._doc,String(i)).toString();case Bt.STYLE:return pr(i,"Style")?gi(i):i;case Bt.SCRIPT:if(pr(i,"Script"))return gi(i);throw new R(5200,!1);case Bt.URL:return pr(i,"URL")?gi(i):Wc(String(i));case Bt.RESOURCE_URL:if(pr(i,"ResourceURL"))return gi(i);throw new R(5201,!1);default:throw new R(5202,!1)}}bypassSecurityTrustHtml(e){return w_(e)}bypassSecurityTrustStyle(e){return D_(e)}bypassSecurityTrustScript(e){return E_(e)}bypassSecurityTrustUrl(e){return S_(e)}bypassSecurityTrustResourceUrl(e){return I_(e)}static \u0275fac=function(i){return new(i||t)(J(K))};static \u0275prov=C({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();var de="primary",Ml=Symbol("RouteTitle"),zv=class{params;constructor(n){this.params=n||{}}has(n){return Object.prototype.hasOwnProperty.call(this.params,n)}get(n){if(this.has(n)){let e=this.params[n];return Array.isArray(e)?e[0]:e}return null}getAll(n){if(this.has(n)){let e=this.params[n];return Array.isArray(e)?e:[e]}return[]}get keys(){return Object.keys(this.params)}};function cs(t){return new zv(t)}function Bv(t,n,e){for(let i=0;i<t.length;i++){let r=t[i],o=n[i];if(r[0]===":")e[r.substring(1)]=o;else if(r!==o.path)return!1}return!0}function aE(t,n,e){let i=e.path.split("/"),r=i.indexOf("**");if(r===-1){if(i.length>t.length||e.pathMatch==="full"&&(n.hasChildren()||i.length<t.length))return null;let c={},l=t.slice(0,i.length);return Bv(i,l,c)?{consumed:l,posParams:c}:null}if(r!==i.lastIndexOf("**"))return null;let o=i.slice(0,r),s=i.slice(r+1);if(o.length+s.length>t.length||e.pathMatch==="full"&&n.hasChildren()&&e.path!=="**")return null;let a={};return!Bv(o,t.slice(0,o.length),a)||!Bv(s,t.slice(t.length-s.length),a)?null:{consumed:t,posParams:a}}function qm(t){return new Promise((n,e)=>{t.pipe(sr()).subscribe({next:i=>n(i),error:i=>e(i)})})}function mP(t,n){if(t.length!==n.length)return!1;for(let e=0;e<t.length;++e)if(!Yi(t[e],n[e]))return!1;return!0}function Yi(t,n){let e=t?$v(t):void 0,i=n?$v(n):void 0;if(!e||!i||e.length!=i.length)return!1;let r;for(let o=0;o<e.length;o++)if(r=e[o],!cE(t[r],n[r]))return!1;return!0}function $v(t){return[...Object.keys(t),...Object.getOwnPropertySymbols(t)]}function cE(t,n){if(Array.isArray(t)&&Array.isArray(n)){if(t.length!==n.length)return!1;let e=[...t].sort(),i=[...n].sort();return e.every((r,o)=>i[o]===r)}else return t===n}function fP(t){return t.length>0?t[t.length-1]:null}function us(t){return lc(t)?t:Xr(t)?ot(Promise.resolve(t)):W(t)}function lE(t){return lc(t)?qm(t):Promise.resolve(t)}var hP={exact:uE,subset:mE},dE={exact:pP,subset:gP,ignored:()=>!0},rb={paths:"exact",fragment:"ignored",matrixParams:"ignored",queryParams:"exact"},yl={paths:"subset",fragment:"ignored",matrixParams:"ignored",queryParams:"subset"};function ob(t,n,e){let i=t instanceof bn?t:n.parseUrl(t);return xt(()=>Gv(n.lastSuccessfulNavigation()?.finalUrl??new bn,i,b(b({},yl),e)))}function Gv(t,n,e){return hP[e.paths](t.root,n.root,e.matrixParams)&&dE[e.queryParams](t.queryParams,n.queryParams)&&!(e.fragment==="exact"&&t.fragment!==n.fragment)}function pP(t,n){return Yi(t,n)}function uE(t,n,e){if(!as(t.segments,n.segments)||!$m(t.segments,n.segments,e)||t.numberOfChildren!==n.numberOfChildren)return!1;for(let i in n.children)if(!t.children[i]||!uE(t.children[i],n.children[i],e))return!1;return!0}function gP(t,n){return Object.keys(n).length<=Object.keys(t).length&&Object.keys(n).every(e=>cE(t[e],n[e]))}function mE(t,n,e){return fE(t,n,n.segments,e)}function fE(t,n,e,i){if(t.segments.length>e.length){let r=t.segments.slice(0,e.length);return!(!as(r,e)||n.hasChildren()||!$m(r,e,i))}else if(t.segments.length===e.length){if(!as(t.segments,e)||!$m(t.segments,e,i))return!1;for(let r in n.children)if(!t.children[r]||!mE(t.children[r],n.children[r],i))return!1;return!0}else{let r=e.slice(0,t.segments.length),o=e.slice(t.segments.length);return!as(t.segments,r)||!$m(t.segments,r,i)||!t.children[de]?!1:fE(t.children[de],n,o,i)}}function $m(t,n,e){return n.every((i,r)=>dE[e](t[r].parameters,i.parameters))}var bn=class{root;queryParams;fragment;_queryParamMap;constructor(n=new Le([],{}),e={},i=null){this.root=n,this.queryParams=e,this.fragment=i}get queryParamMap(){return this._queryParamMap??=cs(this.queryParams),this._queryParamMap}toString(){return bP.serialize(this)}},Le=class{segments;children;parent=null;constructor(n,e){this.segments=n,this.children=e,Object.values(e).forEach(i=>i.parent=this)}hasChildren(){return this.numberOfChildren>0}get numberOfChildren(){return Object.keys(this.children).length}toString(){return Gm(this)}},no=class{path;parameters;_parameterMap;constructor(n,e){this.path=n,this.parameters=e}get parameterMap(){return this._parameterMap??=cs(this.parameters),this._parameterMap}toString(){return pE(this)}};function _P(t,n){return as(t,n)&&t.every((e,i)=>Yi(e.parameters,n[i].parameters))}function as(t,n){return t.length!==n.length?!1:t.every((e,i)=>e.path===n[i].path)}function vP(t,n){let e=[];return Object.entries(t.children).forEach(([i,r])=>{i===de&&(e=e.concat(n(r,i)))}),Object.entries(t.children).forEach(([i,r])=>{i!==de&&(e=e.concat(n(r,i)))}),e}var ya=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275prov=C({token:t,factory:()=>new io,providedIn:"root"})}return t})(),io=class{parse(n){let e=new qv(n);return new bn(e.parseRootSegment(),e.parseQueryParams(),e.parseFragment())}serialize(n){let e=`/${pl(n.root,!0)}`,i=CP(n.queryParams),r=typeof n.fragment=="string"?`#${yP(n.fragment)}`:"";return`${e}${i}${r}`}},bP=new io;function Gm(t){return t.segments.map(n=>pE(n)).join("/")}function pl(t,n){if(!t.hasChildren())return Gm(t);if(n){let e=t.children[de]?pl(t.children[de],!1):"",i=[];return Object.entries(t.children).forEach(([r,o])=>{r!==de&&i.push(`${r}:${pl(o,!1)}`)}),i.length>0?`${e}(${i.join("//")})`:e}else{let e=vP(t,(i,r)=>r===de?[pl(t.children[de],!1)]:[`${r}:${pl(i,!1)}`]);return Object.keys(t.children).length===1&&t.children[de]!=null?`${Gm(t)}/${e[0]}`:`${Gm(t)}/(${e.join("//")})`}}function hE(t){return encodeURIComponent(t).replace(/%40/g,"@").replace(/%3A/gi,":").replace(/%24/g,"$").replace(/%2C/gi,",")}function Um(t){return hE(t).replace(/%3B/gi,";")}function yP(t){return encodeURI(t)}function Wv(t){return hE(t).replace(/\(/g,"%28").replace(/\)/g,"%29").replace(/%26/gi,"&")}function Wm(t){return decodeURIComponent(t)}function tE(t){return Wm(t.replace(/\+/g,"%20"))}function pE(t){return`${Wv(t.path)}${xP(t.parameters)}`}function xP(t){return Object.entries(t).map(([n,e])=>`;${Wv(n)}=${Wv(e)}`).join("")}function CP(t){let n=Object.entries(t).map(([e,i])=>Array.isArray(i)?i.map(r=>`${Um(e)}=${Um(r)}`).join("&"):`${Um(e)}=${Um(i)}`).filter(e=>e);return n.length?`?${n.join("&")}`:""}var wP=/^[^\/()?;#]+/;function Vv(t){let n=t.match(wP);return n?n[0]:""}var DP=/^[^\/()?;=#]+/;function EP(t){let n=t.match(DP);return n?n[0]:""}var SP=/^[^=?&#]+/;function IP(t){let n=t.match(SP);return n?n[0]:""}var kP=/^[^&#]+/;function MP(t){let n=t.match(kP);return n?n[0]:""}var qv=class{url;remaining;constructor(n){this.url=n,this.remaining=n}parseRootSegment(){for(;this.consumeOptional("/"););return this.remaining===""||this.peekStartsWith("?")||this.peekStartsWith("#")?new Le([],{}):new Le([],this.parseChildren())}parseQueryParams(){let n={};if(this.consumeOptional("?"))do this.parseQueryParam(n);while(this.consumeOptional("&"));return n}parseFragment(){return this.consumeOptional("#")?decodeURIComponent(this.remaining):null}parseChildren(n=0){if(n>50)throw new R(4010,!1);if(this.remaining==="")return{};this.consumeOptional("/");let e=[];for(this.peekStartsWith("(")||e.push(this.parseSegment());this.peekStartsWith("/")&&!this.peekStartsWith("//")&&!this.peekStartsWith("/(");)this.capture("/"),e.push(this.parseSegment());let i={};this.peekStartsWith("/(")&&(this.capture("/"),i=this.parseParens(!0,n));let r={};return this.peekStartsWith("(")&&(r=this.parseParens(!1,n)),(e.length>0||Object.keys(i).length>0)&&(r[de]=new Le(e,i)),r}parseSegment(){let n=Vv(this.remaining);if(n===""&&this.peekStartsWith(";"))throw new R(4009,!1);return this.capture(n),new no(Wm(n),this.parseMatrixParams())}parseMatrixParams(){let n={};for(;this.consumeOptional(";");)this.parseParam(n);return n}parseParam(n){let e=EP(this.remaining);if(!e)return;this.capture(e);let i="";if(this.consumeOptional("=")){let r=Vv(this.remaining);r&&(i=r,this.capture(i))}n[Wm(e)]=Wm(i)}parseQueryParam(n){let e=IP(this.remaining);if(!e)return;this.capture(e);let i="";if(this.consumeOptional("=")){let s=MP(this.remaining);s&&(i=s,this.capture(i))}let r=tE(e),o=tE(i);if(n.hasOwnProperty(r)){let s=n[r];Array.isArray(s)||(s=[s],n[r]=s),s.push(o)}else n[r]=o}parseParens(n,e){let i={};for(this.capture("(");!this.consumeOptional(")")&&this.remaining.length>0;){let r=Vv(this.remaining),o=this.remaining[r.length];if(o!=="/"&&o!==")"&&o!==";")throw new R(4010,!1);let s;r.indexOf(":")>-1?(s=r.slice(0,r.indexOf(":")),this.capture(s),this.capture(":")):n&&(s=de);let a=this.parseChildren(e+1);i[s??de]=Object.keys(a).length===1&&a[de]?a[de]:new Le([],a),this.consumeOptional("//")}return i}peekStartsWith(n){return this.remaining.startsWith(n)}consumeOptional(n){return this.peekStartsWith(n)?(this.remaining=this.remaining.substring(n.length),!0):!1}capture(n){if(!this.consumeOptional(n))throw new R(4011,!1)}};function gE(t){return t.segments.length>0?new Le([],{[de]:t}):t}function _E(t){let n={};for(let[i,r]of Object.entries(t.children)){let o=_E(r);if(i===de&&o.segments.length===0&&o.hasChildren())for(let[s,a]of Object.entries(o.children))n[s]=a;else(o.segments.length>0||o.hasChildren())&&(n[i]=o)}let e=new Le(t.segments,n);return TP(e)}function TP(t){if(t.numberOfChildren===1&&t.children[de]){let n=t.children[de];return new Le(t.segments.concat(n.segments),n.children)}return t}function ro(t){return t instanceof bn}function vE(t,n,e=null,i=null,r=new io){let o=bE(t);return yE(o,n,e,i,r)}function bE(t){let n;function e(o){let s={};for(let c of o.children){let l=e(c);s[c.outlet]=l}let a=new Le(o.url,s);return o===t&&(n=a),a}let i=e(t.root),r=gE(i);return n??r}function yE(t,n,e,i,r){let o=t;for(;o.parent;)o=o.parent;if(n.length===0)return Hv(o,o,o,e,i,r);let s=AP(n);if(s.toRoot())return Hv(o,o,new Le([],{}),e,i,r);let a=RP(s,o,t),c=a.processChildren?_l(a.segmentGroup,a.index,s.commands):CE(a.segmentGroup,a.index,s.commands);return Hv(o,a.segmentGroup,c,e,i,r)}function Ym(t){return typeof t=="object"&&t!=null&&!t.outlets&&!t.segmentPath}function xl(t){return typeof t=="object"&&t!=null&&t.outlets}function nE(t,n,e){t||="\u0275";let i=new bn;return i.queryParams={[t]:n},e.parse(e.serialize(i)).queryParams[t]}function Hv(t,n,e,i,r,o){let s={};for(let[l,u]of Object.entries(i??{}))s[l]=Array.isArray(u)?u.map(m=>nE(l,m,o)):nE(l,u,o);let a;t===n?a=e:a=xE(t,n,e);let c=gE(_E(a));return new bn(c,s,r)}function xE(t,n,e){let i={};return Object.entries(t.children).forEach(([r,o])=>{o===n?i[r]=e:i[r]=xE(o,n,e)}),new Le(t.segments,i)}var Zm=class{isAbsolute;numberOfDoubleDots;commands;constructor(n,e,i){if(this.isAbsolute=n,this.numberOfDoubleDots=e,this.commands=i,n&&i.length>0&&Ym(i[0]))throw new R(4003,!1);let r=i.find(xl);if(r&&r!==fP(i))throw new R(4004,!1)}toRoot(){return this.isAbsolute&&this.commands.length===1&&this.commands[0]=="/"}};function AP(t){if(typeof t[0]=="string"&&t.length===1&&t[0]==="/")return new Zm(!0,0,t);let n=0,e=!1,i=t.reduce((r,o,s)=>{if(typeof o=="object"&&o!=null){if(o.outlets){let a={};return Object.entries(o.outlets).forEach(([c,l])=>{a[c]=typeof l=="string"?l.split("/"):l}),[...r,{outlets:a}]}if(o.segmentPath)return[...r,o.segmentPath]}return typeof o!="string"?[...r,o]:s===0?(o.split("/").forEach((a,c)=>{c==0&&a==="."||(c==0&&a===""?e=!0:a===".."?n++:a!=""&&r.push(a))}),r):[...r,o]},[]);return new Zm(e,n,i)}var fa=class{segmentGroup;processChildren;index;constructor(n,e,i){this.segmentGroup=n,this.processChildren=e,this.index=i}};function RP(t,n,e){if(t.isAbsolute)return new fa(n,!0,0);if(!e)return new fa(n,!1,NaN);if(e.parent===null)return new fa(e,!0,0);let i=Ym(t.commands[0])?0:1,r=e.segments.length-1+i;return OP(e,r,t.numberOfDoubleDots)}function OP(t,n,e){let i=t,r=n,o=e;for(;o>r;){if(o-=r,i=i.parent,!i)throw new R(4005,!1);r=i.segments.length}return new fa(i,!1,r-o)}function NP(t){return xl(t[0])?t[0].outlets:{[de]:t}}function CE(t,n,e){if(t??=new Le([],{}),t.segments.length===0&&t.hasChildren())return _l(t,n,e);let i=PP(t,n,e),r=e.slice(i.commandIndex);if(i.match&&i.pathIndex<t.segments.length){let o=new Le(t.segments.slice(0,i.pathIndex),{});return o.children[de]=new Le(t.segments.slice(i.pathIndex),t.children),_l(o,0,r)}else return i.match&&r.length===0?new Le(t.segments,{}):i.match&&!t.hasChildren()?Yv(t,n,e):i.match?_l(t,0,r):Yv(t,n,e)}function _l(t,n,e){if(e.length===0)return new Le(t.segments,{});{let i=NP(e),r={};if(Object.keys(i).some(o=>o!==de)&&t.children[de]&&t.numberOfChildren===1&&t.children[de].segments.length===0){let o=_l(t.children[de],n,e);return new Le(t.segments,o.children)}return Object.entries(i).forEach(([o,s])=>{typeof s=="string"&&(s=[s]),s!==null&&(r[o]=CE(t.children[o],n,s))}),Object.entries(t.children).forEach(([o,s])=>{i[o]===void 0&&(r[o]=s)}),new Le(t.segments,r)}}function PP(t,n,e){let i=0,r=n,o={match:!1,pathIndex:0,commandIndex:0};for(;r<t.segments.length;){if(i>=e.length)return o;let s=t.segments[r],a=e[i];if(xl(a))break;let c=`${a}`,l=i<e.length-1?e[i+1]:null;if(r>0&&c===void 0)break;if(c&&l&&typeof l=="object"&&l.outlets===void 0){if(!rE(c,l,s))return o;i+=2}else{if(!rE(c,{},s))return o;i++}r++}return{match:!0,pathIndex:r,commandIndex:i}}function Yv(t,n,e){let i=t.segments.slice(0,n),r=0;for(;r<e.length;){let o=e[r];if(xl(o)){let c=FP(o.outlets);return new Le(i,c)}if(r===0&&Ym(e[0])){let c=t.segments[n];i.push(new no(c.path,iE(e[0]))),r++;continue}let s=xl(o)?o.outlets[de]:`${o}`,a=r<e.length-1?e[r+1]:null;s&&a&&Ym(a)?(i.push(new no(s,iE(a))),r+=2):(i.push(new no(s,{})),r++)}return new Le(i,{})}function FP(t){let n={};return Object.entries(t).forEach(([e,i])=>{typeof i=="string"&&(i=[i]),i!==null&&(n[e]=Yv(new Le([],{}),0,i))}),n}function iE(t){let n={};return Object.entries(t).forEach(([e,i])=>n[e]=`${i}`),n}function rE(t,n,e){return t==e.path&&Yi(n,e.parameters)}var vl="imperative",qt=(function(t){return t[t.NavigationStart=0]="NavigationStart",t[t.NavigationEnd=1]="NavigationEnd",t[t.NavigationCancel=2]="NavigationCancel",t[t.NavigationError=3]="NavigationError",t[t.RoutesRecognized=4]="RoutesRecognized",t[t.ResolveStart=5]="ResolveStart",t[t.ResolveEnd=6]="ResolveEnd",t[t.GuardsCheckStart=7]="GuardsCheckStart",t[t.GuardsCheckEnd=8]="GuardsCheckEnd",t[t.RouteConfigLoadStart=9]="RouteConfigLoadStart",t[t.RouteConfigLoadEnd=10]="RouteConfigLoadEnd",t[t.ChildActivationStart=11]="ChildActivationStart",t[t.ChildActivationEnd=12]="ChildActivationEnd",t[t.ActivationStart=13]="ActivationStart",t[t.ActivationEnd=14]="ActivationEnd",t[t.Scroll=15]="Scroll",t[t.NavigationSkipped=16]="NavigationSkipped",t})(qt||{}),Vn=class{id;url;constructor(n,e){this.id=n,this.url=e}},ls=class extends Vn{type=qt.NavigationStart;navigationTrigger;restoredState;constructor(n,e,i="imperative",r=null){super(n,e),this.navigationTrigger=i,this.restoredState=r}toString(){return`NavigationStart(id: ${this.id}, url: '${this.url}')`}},xi=class extends Vn{urlAfterRedirects;type=qt.NavigationEnd;constructor(n,e,i){super(n,e),this.urlAfterRedirects=i}toString(){return`NavigationEnd(id: ${this.id}, url: '${this.url}', urlAfterRedirects: '${this.urlAfterRedirects}')`}},ln=(function(t){return t[t.Redirect=0]="Redirect",t[t.SupersededByNewNavigation=1]="SupersededByNewNavigation",t[t.NoDataFromResolver=2]="NoDataFromResolver",t[t.GuardRejected=3]="GuardRejected",t[t.Aborted=4]="Aborted",t})(ln||{}),Cl=(function(t){return t[t.IgnoredSameUrlNavigation=0]="IgnoredSameUrlNavigation",t[t.IgnoredByUrlHandlingStrategy=1]="IgnoredByUrlHandlingStrategy",t})(Cl||{}),ti=class extends Vn{reason;code;type=qt.NavigationCancel;constructor(n,e,i,r){super(n,e),this.reason=i,this.code=r}toString(){return`NavigationCancel(id: ${this.id}, url: '${this.url}')`}};function wE(t){return t instanceof ti&&(t.code===ln.Redirect||t.code===ln.SupersededByNewNavigation)}var br=class extends Vn{reason;code;type=qt.NavigationSkipped;constructor(n,e,i,r){super(n,e),this.reason=i,this.code=r}},ds=class extends Vn{error;target;type=qt.NavigationError;constructor(n,e,i,r){super(n,e),this.error=i,this.target=r}toString(){return`NavigationError(id: ${this.id}, url: '${this.url}', error: ${this.error})`}},wl=class extends Vn{urlAfterRedirects;state;type=qt.RoutesRecognized;constructor(n,e,i,r){super(n,e),this.urlAfterRedirects=i,this.state=r}toString(){return`RoutesRecognized(id: ${this.id}, url: '${this.url}', urlAfterRedirects: '${this.urlAfterRedirects}', state: ${this.state})`}},Qm=class extends Vn{urlAfterRedirects;state;type=qt.GuardsCheckStart;constructor(n,e,i,r){super(n,e),this.urlAfterRedirects=i,this.state=r}toString(){return`GuardsCheckStart(id: ${this.id}, url: '${this.url}', urlAfterRedirects: '${this.urlAfterRedirects}', state: ${this.state})`}},Xm=class extends Vn{urlAfterRedirects;state;shouldActivate;type=qt.GuardsCheckEnd;constructor(n,e,i,r,o){super(n,e),this.urlAfterRedirects=i,this.state=r,this.shouldActivate=o}toString(){return`GuardsCheckEnd(id: ${this.id}, url: '${this.url}', urlAfterRedirects: '${this.urlAfterRedirects}', state: ${this.state}, shouldActivate: ${this.shouldActivate})`}},Km=class extends Vn{urlAfterRedirects;state;type=qt.ResolveStart;constructor(n,e,i,r){super(n,e),this.urlAfterRedirects=i,this.state=r}toString(){return`ResolveStart(id: ${this.id}, url: '${this.url}', urlAfterRedirects: '${this.urlAfterRedirects}', state: ${this.state})`}},Jm=class extends Vn{urlAfterRedirects;state;type=qt.ResolveEnd;constructor(n,e,i,r){super(n,e),this.urlAfterRedirects=i,this.state=r}toString(){return`ResolveEnd(id: ${this.id}, url: '${this.url}', urlAfterRedirects: '${this.urlAfterRedirects}', state: ${this.state})`}},ef=class{route;type=qt.RouteConfigLoadStart;constructor(n){this.route=n}toString(){return`RouteConfigLoadStart(path: ${this.route.path})`}},tf=class{route;type=qt.RouteConfigLoadEnd;constructor(n){this.route=n}toString(){return`RouteConfigLoadEnd(path: ${this.route.path})`}},nf=class{snapshot;type=qt.ChildActivationStart;constructor(n){this.snapshot=n}toString(){return`ChildActivationStart(path: '${this.snapshot.routeConfig&&this.snapshot.routeConfig.path||""}')`}},rf=class{snapshot;type=qt.ChildActivationEnd;constructor(n){this.snapshot=n}toString(){return`ChildActivationEnd(path: '${this.snapshot.routeConfig&&this.snapshot.routeConfig.path||""}')`}},of=class{snapshot;type=qt.ActivationStart;constructor(n){this.snapshot=n}toString(){return`ActivationStart(path: '${this.snapshot.routeConfig&&this.snapshot.routeConfig.path||""}')`}},sf=class{snapshot;type=qt.ActivationEnd;constructor(n){this.snapshot=n}toString(){return`ActivationEnd(path: '${this.snapshot.routeConfig&&this.snapshot.routeConfig.path||""}')`}};var pa=class{},Dl=class{},ga=class{url;navigationBehaviorOptions;constructor(n,e){this.url=n,this.navigationBehaviorOptions=e}};function LP(t){return!(t instanceof pa)&&!(t instanceof ga)&&!(t instanceof Dl)}var af=class{rootInjector;outlet=null;route=null;children;attachRef=null;get injector(){return this.route?.snapshot._environmentInjector??this.rootInjector}constructor(n){this.rootInjector=n,this.children=new xa(this.rootInjector)}},xa=(()=>{class t{rootInjector;contexts=new Map;constructor(e){this.rootInjector=e}onChildOutletCreated(e,i){let r=this.getOrCreateContext(e);r.outlet=i,this.contexts.set(e,r)}onChildOutletDestroyed(e){let i=this.getContext(e);i&&(i.outlet=null,i.attachRef=null)}onOutletDeactivated(){let e=this.contexts;return this.contexts=new Map,e}onOutletReAttached(e){this.contexts=e}getOrCreateContext(e){let i=this.getContext(e);return i||(i=new af(this.rootInjector),this.contexts.set(e,i)),i}getContext(e){return this.contexts.get(e)||null}static \u0275fac=function(i){return new(i||t)(J(He))};static \u0275prov=C({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})(),cf=class{_root;constructor(n){this._root=n}get root(){return this._root.value}parent(n){let e=this.pathFromRoot(n);return e.length>1?e[e.length-2]:null}children(n){let e=Zv(n,this._root);return e?e.children.map(i=>i.value):[]}firstChild(n){let e=Zv(n,this._root);return e&&e.children.length>0?e.children[0].value:null}siblings(n){let e=Qv(n,this._root);return e.length<2?[]:e[e.length-2].children.map(r=>r.value).filter(r=>r!==n)}pathFromRoot(n){return Qv(n,this._root).map(e=>e.value)}};function Zv(t,n){if(t===n.value)return n;for(let e of n.children){let i=Zv(t,e);if(i)return i}return null}function Qv(t,n){if(t===n.value)return[n];for(let e of n.children){let i=Qv(t,e);if(i.length)return i.unshift(n),i}return[]}var Bn=class{value;children;constructor(n,e){this.value=n,this.children=e}toString(){return`TreeNode(${this.value})`}};function ma(t){let n={};return t&&t.children.forEach(e=>n[e.value.outlet]=e),n}var El=class extends cf{snapshot;constructor(n,e){super(n),this.snapshot=e,ab(this,n)}toString(){return this.snapshot.toString()}};function DE(t,n){let e=jP(t,n),i=new Ft([new no("",{})]),r=new Ft({}),o=new Ft({}),s=new Ft({}),a=new Ft(""),c=new yn(i,r,s,a,o,de,t,e.root);return c.snapshot=e.root,new El(new Bn(c,[]),e)}function jP(t,n){let e={},i={},r={},s=new _a([],e,r,"",i,de,t,null,{},n);return new Sl("",new Bn(s,[]))}var yn=class{urlSubject;paramsSubject;queryParamsSubject;fragmentSubject;dataSubject;outlet;component;snapshot;_futureSnapshot;_routerState;_paramMap;_queryParamMap;title;url;params;queryParams;fragment;data;constructor(n,e,i,r,o,s,a,c){this.urlSubject=n,this.paramsSubject=e,this.queryParamsSubject=i,this.fragmentSubject=r,this.dataSubject=o,this.outlet=s,this.component=a,this._futureSnapshot=c,this.title=this.dataSubject?.pipe(Q(l=>l[Ml]))??W(void 0),this.url=n,this.params=e,this.queryParams=i,this.fragment=r,this.data=o}get routeConfig(){return this._futureSnapshot.routeConfig}get root(){return this._routerState.root}get parent(){return this._routerState.parent(this)}get firstChild(){return this._routerState.firstChild(this)}get children(){return this._routerState.children(this)}get pathFromRoot(){return this._routerState.pathFromRoot(this)}get paramMap(){return this._paramMap??=this.params.pipe(Q(n=>cs(n))),this._paramMap}get queryParamMap(){return this._queryParamMap??=this.queryParams.pipe(Q(n=>cs(n))),this._queryParamMap}toString(){return this.snapshot?this.snapshot.toString():`Future(${this._futureSnapshot})`}};function sb(t,n,e="emptyOnly"){let i,{routeConfig:r}=t;return n!==null&&(e==="always"||r?.path===""||!n.component&&!n.routeConfig?.loadComponent)?i={params:b(b({},n.params),t.params),data:b(b({},n.data),t.data),resolve:b(b(b(b({},t.data),n.data),r?.data),t._resolvedData)}:i={params:b({},t.params),data:b({},t.data),resolve:b(b({},t.data),t._resolvedData??{})},r&&SE(r)&&(i.resolve[Ml]=r.title),i}var _a=class{url;params;queryParams;fragment;data;outlet;component;routeConfig;_resolve;_resolvedData;_routerState;_paramMap;_queryParamMap;_environmentInjector;get title(){return this.data?.[Ml]}constructor(n,e,i,r,o,s,a,c,l,u){this.url=n,this.params=e,this.queryParams=i,this.fragment=r,this.data=o,this.outlet=s,this.component=a,this.routeConfig=c,this._resolve=l,this._environmentInjector=u}get root(){return this._routerState.root}get parent(){return this._routerState.parent(this)}get firstChild(){return this._routerState.firstChild(this)}get children(){return this._routerState.children(this)}get pathFromRoot(){return this._routerState.pathFromRoot(this)}get paramMap(){return this._paramMap??=cs(this.params),this._paramMap}get queryParamMap(){return this._queryParamMap??=cs(this.queryParams),this._queryParamMap}toString(){let n=this.url.map(i=>i.toString()).join("/"),e=this.routeConfig?this.routeConfig.path:"";return`Route(url:'${n}', path:'${e}')`}},Sl=class extends cf{url;constructor(n,e){super(e),this.url=n,ab(this,e)}toString(){return EE(this._root)}};function ab(t,n){n.value._routerState=t,n.children.forEach(e=>ab(t,e))}function EE(t){let n=t.children.length>0?` { ${t.children.map(EE).join(", ")} } `:"";return`${t.value}${n}`}function Uv(t){if(t.snapshot){let n=t.snapshot,e=t._futureSnapshot;t.snapshot=e,Yi(n.queryParams,e.queryParams)||t.queryParamsSubject.next(e.queryParams),n.fragment!==e.fragment&&t.fragmentSubject.next(e.fragment),Yi(n.params,e.params)||t.paramsSubject.next(e.params),mP(n.url,e.url)||t.urlSubject.next(e.url),Yi(n.data,e.data)||t.dataSubject.next(e.data)}else t.snapshot=t._futureSnapshot,t.dataSubject.next(t._futureSnapshot.data)}function Xv(t,n){let e=Yi(t.params,n.params)&&_P(t.url,n.url),i=!t.parent!=!n.parent;return e&&!i&&(!t.parent||Xv(t.parent,n.parent))}function SE(t){return typeof t.title=="string"||t.title===null}var IE=new y(""),Tl=(()=>{class t{activated=null;get activatedComponentRef(){return this.activated}_activatedRoute=null;name=de;activateEvents=new U;deactivateEvents=new U;attachEvents=new U;detachEvents=new U;routerOutletData=sa();parentContexts=d(xa);location=d(Vt);changeDetector=d(ve);inputBinder=d(mf,{optional:!0});supportsBindingToComponentInputs=!0;ngOnChanges(e){if(e.name){let{firstChange:i,previousValue:r}=e.name;if(i)return;this.isTrackedInParentContexts(r)&&(this.deactivate(),this.parentContexts.onChildOutletDestroyed(r)),this.initializeOutletWithName()}}ngOnDestroy(){this.isTrackedInParentContexts(this.name)&&this.parentContexts.onChildOutletDestroyed(this.name),this.inputBinder?.unsubscribeFromRouteData(this)}isTrackedInParentContexts(e){return this.parentContexts.getContext(e)?.outlet===this}ngOnInit(){this.initializeOutletWithName()}initializeOutletWithName(){if(this.parentContexts.onChildOutletCreated(this.name,this),this.activated)return;let e=this.parentContexts.getContext(this.name);e?.route&&(e.attachRef?this.attach(e.attachRef,e.route):this.activateWith(e.route,e.injector))}get isActivated(){return!!this.activated}get component(){if(!this.activated)throw new R(4012,!1);return this.activated.instance}get activatedRoute(){if(!this.activated)throw new R(4012,!1);return this._activatedRoute}get activatedRouteData(){return this._activatedRoute?this._activatedRoute.snapshot.data:{}}detach(){if(!this.activated)throw new R(4012,!1);this.location.detach();let e=this.activated;return this.activated=null,this._activatedRoute=null,this.detachEvents.emit(e.instance),e}attach(e,i){this.activated=e,this._activatedRoute=i,this.location.insert(e.hostView),this.inputBinder?.bindActivatedRouteToOutletComponent(this),this.attachEvents.emit(e.instance)}deactivate(){if(this.activated){let e=this.component;this.activated.destroy(),this.activated=null,this._activatedRoute=null,this.deactivateEvents.emit(e)}}activateWith(e,i){if(this.isActivated)throw new R(4013,!1);this._activatedRoute=e;let r=this.location,s=e.snapshot.component,a=this.parentContexts.getOrCreateContext(this.name).children,c=new Kv(e,a,r.injector,this.routerOutletData);this.activated=r.createComponent(s,{index:r.length,injector:c,environmentInjector:i}),this.changeDetector.markForCheck(),this.inputBinder?.bindActivatedRouteToOutletComponent(this),this.activateEvents.emit(this.activated.instance)}static \u0275fac=function(i){return new(i||t)};static \u0275dir=A({type:t,selectors:[["router-outlet"]],inputs:{name:"name",routerOutletData:[1,"routerOutletData"]},outputs:{activateEvents:"activate",deactivateEvents:"deactivate",attachEvents:"attach",detachEvents:"detach"},exportAs:["outlet"],features:[ze]})}return t})(),Kv=class{route;childContexts;parent;outletData;constructor(n,e,i,r){this.route=n,this.childContexts=e,this.parent=i,this.outletData=r}get(n,e){return n===yn?this.route:n===xa?this.childContexts:n===IE?this.outletData:this.parent.get(n,e)}},mf=new y("");var cb=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275cmp=E({type:t,selectors:[["ng-component"]],exportAs:["emptyRouterOutlet"],decls:1,vars:0,template:function(i,r){i&1&&O(0,"router-outlet")},dependencies:[Tl],encapsulation:2})}return t})();function lb(t){let n=t.children&&t.children.map(lb),e=n?Z(b({},t),{children:n}):b({},t);return!e.component&&!e.loadComponent&&(n||e.loadChildren)&&e.outlet&&e.outlet!==de&&(e.component=cb),e}function BP(t,n,e){let i=Il(t,n._root,e?e._root:void 0);return new El(i,n)}function Il(t,n,e){if(e&&t.shouldReuseRoute(n.value,e.value.snapshot)){let i=e.value;i._futureSnapshot=n.value;let r=VP(t,n,e);return new Bn(i,r)}else{if(t.shouldAttach(n.value)){let o=t.retrieve(n.value);if(o!==null){let s=o.route;return s.value._futureSnapshot=n.value,s.children=n.children.map(a=>Il(t,a)),s}}let i=HP(n.value),r=n.children.map(o=>Il(t,o));return new Bn(i,r)}}function VP(t,n,e){return n.children.map(i=>{for(let r of e.children)if(t.shouldReuseRoute(i.value,r.value.snapshot))return Il(t,i,r);return Il(t,i)})}function HP(t){return new yn(new Ft(t.url),new Ft(t.params),new Ft(t.queryParams),new Ft(t.fragment),new Ft(t.data),t.outlet,t.component,t)}var va=class{redirectTo;navigationBehaviorOptions;constructor(n,e){this.redirectTo=n,this.navigationBehaviorOptions=e}},kE="ngNavigationCancelingError";function lf(t,n){let{redirectTo:e,navigationBehaviorOptions:i}=ro(n)?{redirectTo:n,navigationBehaviorOptions:void 0}:n,r=ME(!1,ln.Redirect);return r.url=e,r.navigationBehaviorOptions=i,r}function ME(t,n){let e=new Error(`NavigationCancelingError: ${t||""}`);return e[kE]=!0,e.cancellationCode=n,e}function UP(t){return TE(t)&&ro(t.url)}function TE(t){return!!t&&t[kE]}var Jv=class{routeReuseStrategy;futureState;currState;forwardEvent;inputBindingEnabled;constructor(n,e,i,r,o){this.routeReuseStrategy=n,this.futureState=e,this.currState=i,this.forwardEvent=r,this.inputBindingEnabled=o}activate(n){let e=this.futureState._root,i=this.currState?this.currState._root:null;this.deactivateChildRoutes(e,i,n),Uv(this.futureState.root),this.activateChildRoutes(e,i,n)}deactivateChildRoutes(n,e,i){let r=ma(e);n.children.forEach(o=>{let s=o.value.outlet;this.deactivateRoutes(o,r[s],i),delete r[s]}),Object.values(r).forEach(o=>{this.deactivateRouteAndItsChildren(o,i)})}deactivateRoutes(n,e,i){let r=n.value,o=e?e.value:null;if(r===o)if(r.component){let s=i.getContext(r.outlet);s&&this.deactivateChildRoutes(n,e,s.children)}else this.deactivateChildRoutes(n,e,i);else o&&this.deactivateRouteAndItsChildren(e,i)}deactivateRouteAndItsChildren(n,e){n.value.component&&this.routeReuseStrategy.shouldDetach(n.value.snapshot)?this.detachAndStoreRouteSubtree(n,e):this.deactivateRouteAndOutlet(n,e)}detachAndStoreRouteSubtree(n,e){let i=e.getContext(n.value.outlet),r=i&&n.value.component?i.children:e,o=ma(n);for(let s of Object.values(o))this.deactivateRouteAndItsChildren(s,r);if(i&&i.outlet){let s=i.outlet.detach(),a=i.children.onOutletDeactivated();this.routeReuseStrategy.store(n.value.snapshot,{componentRef:s,route:n,contexts:a})}}deactivateRouteAndOutlet(n,e){let i=e.getContext(n.value.outlet),r=i&&n.value.component?i.children:e,o=ma(n);for(let s of Object.values(o))this.deactivateRouteAndItsChildren(s,r);i&&(i.outlet&&(i.outlet.deactivate(),i.children.onOutletDeactivated()),i.attachRef=null,i.route=null)}activateChildRoutes(n,e,i){let r=ma(e);n.children.forEach(o=>{this.activateRoutes(o,r[o.value.outlet],i),this.forwardEvent(new sf(o.value.snapshot))}),n.children.length&&this.forwardEvent(new rf(n.value.snapshot))}activateRoutes(n,e,i){let r=n.value,o=e?e.value:null;if(Uv(r),r===o)if(r.component){let s=i.getOrCreateContext(r.outlet);this.activateChildRoutes(n,e,s.children)}else this.activateChildRoutes(n,e,i);else if(r.component){let s=i.getOrCreateContext(r.outlet);if(this.routeReuseStrategy.shouldAttach(r.snapshot)){let a=this.routeReuseStrategy.retrieve(r.snapshot);this.routeReuseStrategy.store(r.snapshot,null),s.children.onOutletReAttached(a.contexts),s.attachRef=a.componentRef,s.route=a.route.value,s.outlet&&s.outlet.attach(a.componentRef,a.route.value),Uv(a.route.value),this.activateChildRoutes(n,null,s.children)}else s.attachRef=null,s.route=r,s.outlet&&s.outlet.activateWith(r,s.injector),this.activateChildRoutes(n,null,s.children)}else this.activateChildRoutes(n,null,i)}},df=class{path;route;constructor(n){this.path=n,this.route=this.path[this.path.length-1]}},ha=class{component;route;constructor(n,e){this.component=n,this.route=e}};function zP(t,n,e){let i=t._root,r=n?n._root:null;return gl(i,r,e,[i.value])}function $P(t){let n=t.routeConfig?t.routeConfig.canActivateChild:null;return!n||n.length===0?null:{node:t,guards:n}}function Ca(t,n){let e=Symbol(),i=n.get(t,e);return i===e?typeof t=="function"&&!Lp(t)?t:n.get(t):i}function gl(t,n,e,i,r={canDeactivateChecks:[],canActivateChecks:[]}){let o=ma(n);return t.children.forEach(s=>{GP(s,o[s.value.outlet],e,i.concat([s.value]),r),delete o[s.value.outlet]}),Object.entries(o).forEach(([s,a])=>bl(a,e.getContext(s),r)),r}function GP(t,n,e,i,r={canDeactivateChecks:[],canActivateChecks:[]}){let o=t.value,s=n?n.value:null,a=e?e.getContext(t.value.outlet):null;if(s&&o.routeConfig===s.routeConfig){let c=WP(s,o,o.routeConfig.runGuardsAndResolvers);c?r.canActivateChecks.push(new df(i)):(o.data=s.data,o._resolvedData=s._resolvedData),o.component?gl(t,n,a?a.children:null,i,r):gl(t,n,e,i,r),c&&a&&a.outlet&&a.outlet.isActivated&&r.canDeactivateChecks.push(new ha(a.outlet.component,s))}else s&&bl(n,a,r),r.canActivateChecks.push(new df(i)),o.component?gl(t,null,a?a.children:null,i,r):gl(t,null,e,i,r);return r}function WP(t,n,e){if(typeof e=="function")return $t(n._environmentInjector,()=>e(t,n));switch(e){case"pathParamsChange":return!as(t.url,n.url);case"pathParamsOrQueryParamsChange":return!as(t.url,n.url)||!Yi(t.queryParams,n.queryParams);case"always":return!0;case"paramsOrQueryParamsChange":return!Xv(t,n)||!Yi(t.queryParams,n.queryParams);default:return!Xv(t,n)}}function bl(t,n,e){let i=ma(t),r=t.value;Object.entries(i).forEach(([o,s])=>{r.component?n?bl(s,n.children.getContext(o),e):bl(s,null,e):bl(s,n,e)}),r.component?n&&n.outlet&&n.outlet.isActivated?e.canDeactivateChecks.push(new ha(n.outlet.component,r)):e.canDeactivateChecks.push(new ha(null,r)):e.canDeactivateChecks.push(new ha(null,r))}function Al(t){return typeof t=="function"}function qP(t){return typeof t=="boolean"}function YP(t){return t&&Al(t.canLoad)}function ZP(t){return t&&Al(t.canActivate)}function QP(t){return t&&Al(t.canActivateChild)}function XP(t){return t&&Al(t.canDeactivate)}function KP(t){return t&&Al(t.canMatch)}function AE(t){return t instanceof ai||t?.name==="EmptyError"}var zm=Symbol("INITIAL_VALUE");function ba(){return gt(t=>dc(t.map(n=>n.pipe(ut(1),et(zm)))).pipe(Q(n=>{for(let e of n)if(e!==!0){if(e===zm)return zm;if(e===!1||JP(e))return e}return!0}),De(n=>n!==zm),ut(1)))}function JP(t){return ro(t)||t instanceof va}function RE(t){return t.aborted?W(void 0).pipe(ut(1)):new ce(n=>{let e=()=>{n.next(),n.complete()};return t.addEventListener("abort",e),()=>t.removeEventListener("abort",e)})}function OE(t){return me(RE(t))}function eF(t){return tn(n=>{let{targetSnapshot:e,currentSnapshot:i,guards:{canActivateChecks:r,canDeactivateChecks:o}}=n;return o.length===0&&r.length===0?W(Z(b({},n),{guardsResult:!0})):tF(o,e,i).pipe(tn(s=>s&&qP(s)?nF(e,r,t):W(s)),Q(s=>Z(b({},n),{guardsResult:s})))})}function tF(t,n,e){return ot(t).pipe(tn(i=>aF(i.component,i.route,e,n)),sr(i=>i!==!0,!0))}function nF(t,n,e){return ot(n).pipe(No(i=>Lr(rF(i.route.parent,e),iF(i.route,e),sF(t,i.path),oF(t,i.route))),sr(i=>i!==!0,!0))}function iF(t,n){return t!==null&&n&&n(new of(t)),W(!0)}function rF(t,n){return t!==null&&n&&n(new nf(t)),W(!0)}function oF(t,n){let e=n.routeConfig?n.routeConfig.canActivate:null;if(!e||e.length===0)return W(!0);let i=e.map(r=>ci(()=>{let o=n._environmentInjector,s=Ca(r,o),a=ZP(s)?s.canActivate(n,t):$t(o,()=>s(n,t));return us(a).pipe(sr())}));return W(i).pipe(ba())}function sF(t,n){let e=n[n.length-1],r=n.slice(0,n.length-1).reverse().map(o=>$P(o)).filter(o=>o!==null).map(o=>ci(()=>{let s=o.guards.map(a=>{let c=o.node._environmentInjector,l=Ca(a,c),u=QP(l)?l.canActivateChild(e,t):$t(c,()=>l(e,t));return us(u).pipe(sr())});return W(s).pipe(ba())}));return W(r).pipe(ba())}function aF(t,n,e,i){let r=n&&n.routeConfig?n.routeConfig.canDeactivate:null;if(!r||r.length===0)return W(!0);let o=r.map(s=>{let a=n._environmentInjector,c=Ca(s,a),l=XP(c)?c.canDeactivate(t,n,e,i):$t(a,()=>c(t,n,e,i));return us(l).pipe(sr())});return W(o).pipe(ba())}function cF(t,n,e,i,r){let o=n.canLoad;if(o===void 0||o.length===0)return W(!0);let s=o.map(a=>{let c=Ca(a,t),l=YP(c)?c.canLoad(n,e):$t(t,()=>c(n,e)),u=us(l);return r?u.pipe(OE(r)):u});return W(s).pipe(ba(),NE(i))}function NE(t){return gp(st(n=>{if(typeof n!="boolean")throw lf(t,n)}),Q(n=>n===!0))}function lF(t,n,e,i,r,o){let s=n.canMatch;if(!s||s.length===0)return W(!0);let a=s.map(c=>{let l=Ca(c,t),u=KP(l)?l.canMatch(n,e,r):$t(t,()=>l(n,e,r));return us(u).pipe(OE(o))});return W(a).pipe(ba(),NE(i))}var vr=class t extends Error{segmentGroup;constructor(n){super(),this.segmentGroup=n||null,Object.setPrototypeOf(this,t.prototype)}},kl=class t extends Error{urlTree;constructor(n){super(),this.urlTree=n,Object.setPrototypeOf(this,t.prototype)}};function dF(t){throw new R(4e3,!1)}function uF(t){throw ME(!1,ln.GuardRejected)}var eb=class{urlSerializer;urlTree;constructor(n,e){this.urlSerializer=n,this.urlTree=e}async lineralizeSegments(n,e){let i=[],r=e.root;for(;;){if(i=i.concat(r.segments),r.numberOfChildren===0)return i;if(r.numberOfChildren>1||!r.children[de])throw dF(`${n.redirectTo}`);r=r.children[de]}}async applyRedirectCommands(n,e,i,r,o){let s=await mF(e,r,o);if(s instanceof bn)throw new kl(s);let a=this.applyRedirectCreateUrlTree(s,this.urlSerializer.parse(s),n,i);if(s[0]==="/")throw new kl(a);return a}applyRedirectCreateUrlTree(n,e,i,r){let o=this.createSegmentGroup(n,e.root,i,r);return new bn(o,this.createQueryParams(e.queryParams,this.urlTree.queryParams),e.fragment)}createQueryParams(n,e){let i={};return Object.entries(n).forEach(([r,o])=>{if(typeof o=="string"&&o[0]===":"){let a=o.substring(1);i[r]=e[a]}else i[r]=o}),i}createSegmentGroup(n,e,i,r){let o=this.createSegments(n,e.segments,i,r),s={};return Object.entries(e.children).forEach(([a,c])=>{s[a]=this.createSegmentGroup(n,c,i,r)}),new Le(o,s)}createSegments(n,e,i,r){return e.map(o=>o.path[0]===":"?this.findPosParam(n,o,r):this.findOrReturn(o,i))}findPosParam(n,e,i){let r=i[e.path.substring(1)];if(!r)throw new R(4001,!1);return r}findOrReturn(n,e){let i=0;for(let r of e){if(r.path===n.path)return e.splice(i),r;i++}return n}};function mF(t,n,e){if(typeof t=="string")return Promise.resolve(t);let i=t;return qm(us($t(e,()=>i(n))))}function fF(t,n){return t.providers&&!t._injector&&(t._injector=Kc(t.providers,n,`Route: ${t.path}`)),t._injector??n}function yi(t){return t.outlet||de}function hF(t,n){let e=t.filter(i=>yi(i)===n);return e.push(...t.filter(i=>yi(i)!==n)),e}var tb={matched:!1,consumedSegments:[],remainingSegments:[],parameters:{},positionalParamSegments:{}};function PE(t){return{routeConfig:t.routeConfig,url:t.url,params:t.params,queryParams:t.queryParams,fragment:t.fragment,data:t.data,outlet:t.outlet,title:t.title,paramMap:t.paramMap,queryParamMap:t.queryParamMap}}function pF(t,n,e,i,r,o,s){let a=FE(t,n,e);if(!a.matched)return W(a);let c=PE(o(a));return i=fF(n,i),lF(i,n,e,r,c,s).pipe(Q(l=>l===!0?a:b({},tb)))}function FE(t,n,e){if(n.path==="")return n.pathMatch==="full"&&(t.hasChildren()||e.length>0)?b({},tb):{matched:!0,consumedSegments:[],remainingSegments:e,parameters:{},positionalParamSegments:{}};let r=(n.matcher||aE)(e,t,n);if(!r)return b({},tb);let o={};Object.entries(r.posParams??{}).forEach(([a,c])=>{o[a]=c.path});let s=r.consumed.length>0?b(b({},o),r.consumed[r.consumed.length-1].parameters):o;return{matched:!0,consumedSegments:r.consumed,remainingSegments:e.slice(r.consumed.length),parameters:s,positionalParamSegments:r.posParams??{}}}function oE(t,n,e,i,r){return e.length>0&&vF(t,e,i,r)?{segmentGroup:new Le(n,_F(i,new Le(e,t.children))),slicedSegments:[]}:e.length===0&&bF(t,e,i)?{segmentGroup:new Le(t.segments,gF(t,e,i,t.children)),slicedSegments:e}:{segmentGroup:new Le(t.segments,t.children),slicedSegments:e}}function gF(t,n,e,i){let r={};for(let o of e)if(ff(t,n,o)&&!i[yi(o)]){let s=new Le([],{});r[yi(o)]=s}return b(b({},i),r)}function _F(t,n){let e={};e[de]=n;for(let i of t)if(i.path===""&&yi(i)!==de){let r=new Le([],{});e[yi(i)]=r}return e}function vF(t,n,e,i){return e.some(r=>!ff(t,n,r)||!(yi(r)!==de)?!1:!(i!==void 0&&yi(r)===i))}function bF(t,n,e){return e.some(i=>ff(t,n,i))}function ff(t,n,e){return(t.hasChildren()||n.length>0)&&e.pathMatch==="full"?!1:e.path===""}function yF(t,n,e){return n.length===0&&!t.children[e]}var nb=class{};async function xF(t,n,e,i,r,o,s="emptyOnly",a){return new ib(t,n,e,i,r,s,o,a).recognize()}var CF=31,ib=class{injector;configLoader;rootComponentType;config;urlTree;paramsInheritanceStrategy;urlSerializer;abortSignal;applyRedirects;absoluteRedirectCount=0;allowRedirects=!0;constructor(n,e,i,r,o,s,a,c){this.injector=n,this.configLoader=e,this.rootComponentType=i,this.config=r,this.urlTree=o,this.paramsInheritanceStrategy=s,this.urlSerializer=a,this.abortSignal=c,this.applyRedirects=new eb(this.urlSerializer,this.urlTree)}noMatchError(n){return new R(4002,`'${n.segmentGroup}'`)}async recognize(){let n=oE(this.urlTree.root,[],[],this.config).segmentGroup,{children:e,rootSnapshot:i}=await this.match(n),r=new Bn(i,e),o=new Sl("",r),s=vE(i,[],this.urlTree.queryParams,this.urlTree.fragment);return s.queryParams=this.urlTree.queryParams,o.url=this.urlSerializer.serialize(s),{state:o,tree:s}}async match(n){let e=new _a([],Object.freeze({}),Object.freeze(b({},this.urlTree.queryParams)),this.urlTree.fragment,Object.freeze({}),de,this.rootComponentType,null,{},this.injector);try{return{children:await this.processSegmentGroup(this.injector,this.config,n,de,e),rootSnapshot:e}}catch(i){if(i instanceof kl)return this.urlTree=i.urlTree,this.match(i.urlTree.root);throw i instanceof vr?this.noMatchError(i):i}}async processSegmentGroup(n,e,i,r,o){if(i.segments.length===0&&i.hasChildren())return this.processChildren(n,e,i,o);let s=await this.processSegment(n,e,i,i.segments,r,!0,o);return s instanceof Bn?[s]:[]}async processChildren(n,e,i,r){let o=[];for(let c of Object.keys(i.children))c==="primary"?o.unshift(c):o.push(c);let s=[];for(let c of o){let l=i.children[c],u=hF(e,c),m=await this.processSegmentGroup(n,u,l,c,r);s.push(...m)}let a=LE(s);return wF(a),a}async processSegment(n,e,i,r,o,s,a){for(let c of e)try{return await this.processSegmentAgainstRoute(c._injector??n,e,c,i,r,o,s,a)}catch(l){if(l instanceof vr||AE(l))continue;throw l}if(yF(i,r,o))return new nb;throw new vr(i)}async processSegmentAgainstRoute(n,e,i,r,o,s,a,c){if(yi(i)!==s&&(s===de||!ff(r,o,i)))throw new vr(r);if(i.redirectTo===void 0)return this.matchSegmentAgainstRoute(n,r,i,o,s,c);if(this.allowRedirects&&a)return this.expandSegmentAgainstRouteUsingRedirect(n,r,e,i,o,s,c);throw new vr(r)}async expandSegmentAgainstRouteUsingRedirect(n,e,i,r,o,s,a){let{matched:c,parameters:l,consumedSegments:u,positionalParamSegments:m,remainingSegments:g}=FE(e,r,o);if(!c)throw new vr(e);typeof r.redirectTo=="string"&&r.redirectTo[0]==="/"&&(this.absoluteRedirectCount++,this.absoluteRedirectCount>CF&&(this.allowRedirects=!1));let v=this.createSnapshot(n,r,o,l,a);if(this.abortSignal.aborted)throw new Error(this.abortSignal.reason);let x=await this.applyRedirects.applyRedirectCommands(u,r.redirectTo,m,PE(v),n),F=await this.applyRedirects.lineralizeSegments(r,x);return this.processSegment(n,i,e,F.concat(g),s,!1,a)}createSnapshot(n,e,i,r,o){let s=new _a(i,r,Object.freeze(b({},this.urlTree.queryParams)),this.urlTree.fragment,EF(e),yi(e),e.component??e._loadedComponent??null,e,SF(e),n),a=sb(s,o,this.paramsInheritanceStrategy);return s.params=Object.freeze(a.params),s.data=Object.freeze(a.data),s}async matchSegmentAgainstRoute(n,e,i,r,o,s){if(this.abortSignal.aborted)throw new Error(this.abortSignal.reason);let a=Dt=>this.createSnapshot(n,i,Dt.consumedSegments,Dt.parameters,s),c=await qm(pF(e,i,r,n,this.urlSerializer,a,this.abortSignal));if(i.path==="**"&&(e.children={}),!c?.matched)throw new vr(e);n=i._injector??n;let{routes:l}=await this.getChildConfig(n,i,r),u=i._loadedInjector??n,{parameters:m,consumedSegments:g,remainingSegments:v}=c,x=this.createSnapshot(n,i,g,m,s),{segmentGroup:F,slicedSegments:j}=oE(e,g,v,l,o);if(j.length===0&&F.hasChildren()){let Dt=await this.processChildren(u,l,F,x);return new Bn(x,Dt)}if(l.length===0&&j.length===0)return new Bn(x,[]);let Y=yi(i)===o,be=await this.processSegment(u,l,F,j,Y?de:o,!0,x);return new Bn(x,be instanceof Bn?[be]:[])}async getChildConfig(n,e,i){if(e.children)return{routes:e.children,injector:n};if(e.loadChildren){if(e._loadedRoutes!==void 0){let o=e._loadedNgModuleFactory;return o&&!e._loadedInjector&&(e._loadedInjector=o.create(n).injector),{routes:e._loadedRoutes,injector:e._loadedInjector}}if(this.abortSignal.aborted)throw new Error(this.abortSignal.reason);if(await qm(cF(n,e,i,this.urlSerializer,this.abortSignal))){let o=await this.configLoader.loadChildren(n,e);return e._loadedRoutes=o.routes,e._loadedInjector=o.injector,e._loadedNgModuleFactory=o.factory,o}throw uF(e)}return{routes:[],injector:n}}};function wF(t){t.sort((n,e)=>n.value.outlet===de?-1:e.value.outlet===de?1:n.value.outlet.localeCompare(e.value.outlet))}function DF(t){let n=t.value.routeConfig;return n&&n.path===""}function LE(t){let n=[],e=new Set;for(let i of t){if(!DF(i)){n.push(i);continue}let r=n.find(o=>i.value.routeConfig===o.value.routeConfig);r!==void 0?(r.children.push(...i.children),e.add(r)):n.push(i)}for(let i of e){let r=LE(i.children);n.push(new Bn(i.value,r))}return n.filter(i=>!e.has(i))}function EF(t){return t.data||{}}function SF(t){return t.resolve||{}}function IF(t,n,e,i,r,o,s){return tn(async a=>{let{state:c,tree:l}=await xF(t,n,e,i,a.extractedUrl,r,o,s);return Z(b({},a),{targetSnapshot:c,urlAfterRedirects:l})})}function kF(t){return tn(n=>{let{targetSnapshot:e,guards:{canActivateChecks:i}}=n;if(!i.length)return W(n);let r=new Set(i.map(a=>a.route)),o=new Set;for(let a of r)if(!o.has(a))for(let c of jE(a))o.add(c);let s=0;return ot(o).pipe(No(a=>r.has(a)?MF(a,e,t):(a.data=sb(a,a.parent,t).resolve,W(void 0))),st(()=>s++),Kd(1),tn(a=>s===o.size?W(n):St))})}function jE(t){let n=t.children.map(e=>jE(e)).flat();return[t,...n]}function MF(t,n,e){let i=t.routeConfig,r=t._resolve;return i?.title!==void 0&&!SE(i)&&(r[Ml]=i.title),ci(()=>(t.data=sb(t,t.parent,e).resolve,TF(r,t,n).pipe(Q(o=>(t._resolvedData=o,t.data=b(b({},t.data),o),null)))))}function TF(t,n,e){let i=$v(t);if(i.length===0)return W({});let r={};return ot(i).pipe(tn(o=>AF(t[o],n,e).pipe(sr(),st(s=>{if(s instanceof va)throw lf(new io,s);r[o]=s}))),Kd(1),Q(()=>r),li(o=>AE(o)?St:Ao(o)))}function AF(t,n,e){let i=n._environmentInjector,r=Ca(t,i),o=r.resolve?r.resolve(n,e):$t(i,()=>r(n,e));return us(o)}function sE(t){return gt(n=>{let e=t(n);return e?ot(e).pipe(Q(()=>n)):W(n)})}var db=(()=>{class t{buildTitle(e){let i,r=e.root;for(;r!==void 0;)i=this.getResolvedTitleForRoute(r)??i,r=r.children.find(o=>o.outlet===de);return i}getResolvedTitleForRoute(e){return e.data[Ml]}static \u0275fac=function(i){return new(i||t)};static \u0275prov=C({token:t,factory:()=>d(BE),providedIn:"root"})}return t})(),BE=(()=>{class t extends db{title;constructor(e){super(),this.title=e}updateTitle(e){let i=this.buildTitle(e);i!==void 0&&this.title.setTitle(i)}static \u0275fac=function(i){return new(i||t)(J(JD))};static \u0275prov=C({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})(),wa=new y("",{factory:()=>({})}),Rl=new y(""),VE=(()=>{class t{componentLoaders=new WeakMap;childrenLoaders=new WeakMap;onLoadStartListener;onLoadEndListener;compiler=d(lv);async loadComponent(e,i){if(this.componentLoaders.get(i))return this.componentLoaders.get(i);if(i._loadedComponent)return Promise.resolve(i._loadedComponent);this.onLoadStartListener&&this.onLoadStartListener(i);let r=(async()=>{try{let o=await lE($t(e,()=>i.loadComponent())),s=await zE(UE(o));return this.onLoadEndListener&&this.onLoadEndListener(i),i._loadedComponent=s,s}finally{this.componentLoaders.delete(i)}})();return this.componentLoaders.set(i,r),r}loadChildren(e,i){if(this.childrenLoaders.get(i))return this.childrenLoaders.get(i);if(i._loadedRoutes)return Promise.resolve({routes:i._loadedRoutes,injector:i._loadedInjector});this.onLoadStartListener&&this.onLoadStartListener(i);let r=(async()=>{try{let o=await HE(i,this.compiler,e,this.onLoadEndListener);return i._loadedRoutes=o.routes,i._loadedInjector=o.injector,i._loadedNgModuleFactory=o.factory,o}finally{this.childrenLoaders.delete(i)}})();return this.childrenLoaders.set(i,r),r}static \u0275fac=function(i){return new(i||t)};static \u0275prov=C({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();async function HE(t,n,e,i){let r=await lE($t(e,()=>t.loadChildren())),o=await zE(UE(r)),s;o instanceof hm||Array.isArray(o)?s=o:s=await n.compileModuleAsync(o),i&&i(t);let a,c,l=!1,u;return Array.isArray(s)?(c=s,l=!0):(a=s.create(e).injector,u=s,c=a.get(Rl,[],{optional:!0,self:!0}).flat()),{routes:c.map(lb),injector:a,factory:u}}function RF(t){return t&&typeof t=="object"&&"default"in t}function UE(t){return RF(t)?t.default:t}async function zE(t){return t}var hf=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275prov=C({token:t,factory:()=>d(OF),providedIn:"root"})}return t})(),OF=(()=>{class t{shouldProcessUrl(e){return!0}extract(e){return e}merge(e,i){return e}static \u0275fac=function(i){return new(i||t)};static \u0275prov=C({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})(),$E=new y("");var NF=()=>{},GE=new y(""),WE=(()=>{class t{currentNavigation=M(null,{equal:()=>!1});currentTransition=null;lastSuccessfulNavigation=M(null);events=new I;transitionAbortWithErrorSubject=new I;configLoader=d(VE);environmentInjector=d(He);destroyRef=d(on);urlSerializer=d(ya);rootContexts=d(xa);location=d(Gi);inputBindingEnabled=d(mf,{optional:!0})!==null;titleStrategy=d(db);options=d(wa,{optional:!0})||{};paramsInheritanceStrategy=this.options.paramsInheritanceStrategy||"emptyOnly";urlHandlingStrategy=d(hf);createViewTransition=d($E,{optional:!0});navigationErrorHandler=d(GE,{optional:!0});navigationId=0;get hasRequestedNavigation(){return this.navigationId!==0}transitions;afterPreactivation=()=>W(void 0);rootComponentType=null;destroyed=!1;constructor(){let e=r=>this.events.next(new ef(r)),i=r=>this.events.next(new tf(r));this.configLoader.onLoadEndListener=i,this.configLoader.onLoadStartListener=e,this.destroyRef.onDestroy(()=>{this.destroyed=!0})}complete(){this.transitions?.complete()}handleNavigationRequest(e){let i=++this.navigationId;Ne(()=>{this.transitions?.next(Z(b({},e),{extractedUrl:this.urlHandlingStrategy.extract(e.rawUrl),targetSnapshot:null,targetRouterState:null,guards:{canActivateChecks:[],canDeactivateChecks:[]},guardsResult:null,id:i,routesRecognizeHandler:{},beforeActivateHandler:{}}))})}setupNavigations(e){return this.transitions=new Ft(null),this.transitions.pipe(De(i=>i!==null),gt(i=>{let r=!1,o=new AbortController,s=()=>!r&&this.currentTransition?.id===i.id;return W(i).pipe(gt(a=>{if(this.navigationId>i.id)return this.cancelNavigationTransition(i,"",ln.SupersededByNewNavigation),St;this.currentTransition=i;let c=this.lastSuccessfulNavigation();this.currentNavigation.set({id:a.id,initialUrl:a.rawUrl,extractedUrl:a.extractedUrl,targetBrowserUrl:typeof a.extras.browserUrl=="string"?this.urlSerializer.parse(a.extras.browserUrl):a.extras.browserUrl,trigger:a.source,extras:a.extras,previousNavigation:c?Z(b({},c),{previousNavigation:null}):null,abort:()=>o.abort(),routesRecognizeHandler:a.routesRecognizeHandler,beforeActivateHandler:a.beforeActivateHandler});let l=!e.navigated||this.isUpdatingInternalState()||this.isUpdatedBrowserUrl(),u=a.extras.onSameUrlNavigation??e.onSameUrlNavigation;if(!l&&u!=="reload")return this.events.next(new br(a.id,this.urlSerializer.serialize(a.rawUrl),"",Cl.IgnoredSameUrlNavigation)),a.resolve(!1),St;if(this.urlHandlingStrategy.shouldProcessUrl(a.rawUrl))return W(a).pipe(gt(m=>(this.events.next(new ls(m.id,this.urlSerializer.serialize(m.extractedUrl),m.source,m.restoredState)),m.id!==this.navigationId?St:Promise.resolve(m))),IF(this.environmentInjector,this.configLoader,this.rootComponentType,e.config,this.urlSerializer,this.paramsInheritanceStrategy,o.signal),st(m=>{i.targetSnapshot=m.targetSnapshot,i.urlAfterRedirects=m.urlAfterRedirects,this.currentNavigation.update(g=>(g.finalUrl=m.urlAfterRedirects,g)),this.events.next(new Dl)}),gt(m=>ot(i.routesRecognizeHandler.deferredHandle??W(void 0)).pipe(Q(()=>m))),st(()=>{let m=new wl(a.id,this.urlSerializer.serialize(a.extractedUrl),this.urlSerializer.serialize(a.urlAfterRedirects),a.targetSnapshot);this.events.next(m)}));if(l&&this.urlHandlingStrategy.shouldProcessUrl(a.currentRawUrl)){let{id:m,extractedUrl:g,source:v,restoredState:x,extras:F}=a,j=new ls(m,this.urlSerializer.serialize(g),v,x);this.events.next(j);let Y=DE(this.rootComponentType,this.environmentInjector).snapshot;return this.currentTransition=i=Z(b({},a),{targetSnapshot:Y,urlAfterRedirects:g,extras:Z(b({},F),{skipLocationChange:!1,replaceUrl:!1})}),this.currentNavigation.update(be=>(be.finalUrl=g,be)),W(i)}else return this.events.next(new br(a.id,this.urlSerializer.serialize(a.extractedUrl),"",Cl.IgnoredByUrlHandlingStrategy)),a.resolve(!1),St}),Q(a=>{let c=new Qm(a.id,this.urlSerializer.serialize(a.extractedUrl),this.urlSerializer.serialize(a.urlAfterRedirects),a.targetSnapshot);return this.events.next(c),this.currentTransition=i=Z(b({},a),{guards:zP(a.targetSnapshot,a.currentSnapshot,this.rootContexts)}),i}),eF(a=>this.events.next(a)),gt(a=>{if(i.guardsResult=a.guardsResult,a.guardsResult&&typeof a.guardsResult!="boolean")throw lf(this.urlSerializer,a.guardsResult);let c=new Xm(a.id,this.urlSerializer.serialize(a.extractedUrl),this.urlSerializer.serialize(a.urlAfterRedirects),a.targetSnapshot,!!a.guardsResult);if(this.events.next(c),!s())return St;if(!a.guardsResult)return this.cancelNavigationTransition(a,"",ln.GuardRejected),St;if(a.guards.canActivateChecks.length===0)return W(a);let l=new Km(a.id,this.urlSerializer.serialize(a.extractedUrl),this.urlSerializer.serialize(a.urlAfterRedirects),a.targetSnapshot);if(this.events.next(l),!s())return St;let u=!1;return W(a).pipe(kF(this.paramsInheritanceStrategy),st({next:()=>{u=!0;let m=new Jm(a.id,this.urlSerializer.serialize(a.extractedUrl),this.urlSerializer.serialize(a.urlAfterRedirects),a.targetSnapshot);this.events.next(m)},complete:()=>{u||this.cancelNavigationTransition(a,"",ln.NoDataFromResolver)}}))}),sE(a=>{let c=u=>{let m=[];if(u.routeConfig?._loadedComponent)u.component=u.routeConfig?._loadedComponent;else if(u.routeConfig?.loadComponent){let g=u._environmentInjector;m.push(this.configLoader.loadComponent(g,u.routeConfig).then(v=>{u.component=v}))}for(let g of u.children)m.push(...c(g));return m},l=c(a.targetSnapshot.root);return l.length===0?W(a):ot(Promise.all(l).then(()=>a))}),sE(()=>this.afterPreactivation()),gt(()=>{let{currentSnapshot:a,targetSnapshot:c}=i,l=this.createViewTransition?.(this.environmentInjector,a.root,c.root);return l?ot(l).pipe(Q(()=>i)):W(i)}),ut(1),gt(a=>{let c=BP(e.routeReuseStrategy,a.targetSnapshot,a.currentRouterState);this.currentTransition=i=a=Z(b({},a),{targetRouterState:c}),this.currentNavigation.update(u=>(u.targetRouterState=c,u)),this.events.next(new pa);let l=i.beforeActivateHandler.deferredHandle;return l?ot(l.then(()=>a)):W(a)}),st(a=>{new Jv(e.routeReuseStrategy,i.targetRouterState,i.currentRouterState,c=>this.events.next(c),this.inputBindingEnabled).activate(this.rootContexts),s()&&(r=!0,this.currentNavigation.update(c=>(c.abort=NF,c)),this.lastSuccessfulNavigation.set(Ne(this.currentNavigation)),this.events.next(new xi(a.id,this.urlSerializer.serialize(a.extractedUrl),this.urlSerializer.serialize(a.urlAfterRedirects))),this.titleStrategy?.updateTitle(a.targetRouterState.snapshot),a.resolve(!0))}),me(RE(o.signal).pipe(De(()=>!r&&!i.targetRouterState),st(()=>{this.cancelNavigationTransition(i,o.signal.reason+"",ln.Aborted)}))),st({complete:()=>{r=!0}}),me(this.transitionAbortWithErrorSubject.pipe(st(a=>{throw a}))),or(()=>{o.abort(),r||this.cancelNavigationTransition(i,"",ln.SupersededByNewNavigation),this.currentTransition?.id===i.id&&(this.currentNavigation.set(null),this.currentTransition=null)}),li(a=>{if(r=!0,this.destroyed)return i.resolve(!1),St;if(TE(a))this.events.next(new ti(i.id,this.urlSerializer.serialize(i.extractedUrl),a.message,a.cancellationCode)),UP(a)?this.events.next(new ga(a.url,a.navigationBehaviorOptions)):i.resolve(!1);else{let c=new ds(i.id,this.urlSerializer.serialize(i.extractedUrl),a,i.targetSnapshot??void 0);try{let l=$t(this.environmentInjector,()=>this.navigationErrorHandler?.(c));if(l instanceof va){let{message:u,cancellationCode:m}=lf(this.urlSerializer,l);this.events.next(new ti(i.id,this.urlSerializer.serialize(i.extractedUrl),u,m)),this.events.next(new ga(l.redirectTo,l.navigationBehaviorOptions))}else throw this.events.next(c),a}catch(l){this.options.resolveNavigationPromiseOnError?i.resolve(!1):i.reject(l)}}return St}))}))}cancelNavigationTransition(e,i,r){let o=new ti(e.id,this.urlSerializer.serialize(e.extractedUrl),i,r);this.events.next(o),e.resolve(!1)}isUpdatingInternalState(){return this.currentTransition?.extractedUrl.toString()!==this.currentTransition?.currentUrlTree.toString()}isUpdatedBrowserUrl(){let e=this.urlHandlingStrategy.extract(this.urlSerializer.parse(this.location.path(!0))),i=Ne(this.currentNavigation),r=i?.targetBrowserUrl??i?.extractedUrl;return e.toString()!==r?.toString()&&!i?.extras.skipLocationChange}static \u0275fac=function(i){return new(i||t)};static \u0275prov=C({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();function PF(t){return t!==vl}var qE=new y("");var YE=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275prov=C({token:t,factory:()=>d(FF),providedIn:"root"})}return t})(),uf=class{shouldDetach(n){return!1}store(n,e){}shouldAttach(n){return!1}retrieve(n){return null}shouldReuseRoute(n,e){return n.routeConfig===e.routeConfig}shouldDestroyInjector(n){return!0}},FF=(()=>{class t extends uf{static \u0275fac=(()=>{let e;return function(r){return(e||(e=ct(t)))(r||t)}})();static \u0275prov=C({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})(),pf=(()=>{class t{urlSerializer=d(ya);options=d(wa,{optional:!0})||{};canceledNavigationResolution=this.options.canceledNavigationResolution||"replace";location=d(Gi);urlHandlingStrategy=d(hf);urlUpdateStrategy=this.options.urlUpdateStrategy||"deferred";currentUrlTree=new bn;getCurrentUrlTree(){return this.currentUrlTree}rawUrlTree=this.currentUrlTree;getRawUrlTree(){return this.rawUrlTree}createBrowserPath({finalUrl:e,initialUrl:i,targetBrowserUrl:r}){let o=e!==void 0?this.urlHandlingStrategy.merge(e,i):i,s=r??o;return s instanceof bn?this.urlSerializer.serialize(s):s}routerUrlState(e){return e?.targetBrowserUrl===void 0||e?.finalUrl===void 0?{}:{\u0275routerUrl:this.urlSerializer.serialize(e.finalUrl)}}commitTransition({targetRouterState:e,finalUrl:i,initialUrl:r}){i&&e?(this.currentUrlTree=i,this.rawUrlTree=this.urlHandlingStrategy.merge(i,r),this.routerState=e):this.rawUrlTree=r}routerState=DE(null,d(He));getRouterState(){return this.routerState}_stateMemento=this.createStateMemento();get stateMemento(){return this._stateMemento}updateStateMemento(){this._stateMemento=this.createStateMemento()}createStateMemento(){return{rawUrlTree:this.rawUrlTree,currentUrlTree:this.currentUrlTree,routerState:this.routerState}}restoredState(){return this.location.getState()}static \u0275fac=function(i){return new(i||t)};static \u0275prov=C({token:t,factory:()=>d(LF),providedIn:"root"})}return t})(),LF=(()=>{class t extends pf{currentPageId=0;lastSuccessfulId=-1;get browserPageId(){return this.canceledNavigationResolution!=="computed"?this.currentPageId:this.restoredState()?.\u0275routerPageId??this.currentPageId}registerNonRouterCurrentEntryChangeListener(e){return this.location.subscribe(i=>{i.type==="popstate"&&setTimeout(()=>{e(i.url,i.state,"popstate",{replaceUrl:!0})})})}handleRouterEvent(e,i){e instanceof ls?this.updateStateMemento():e instanceof br?this.commitTransition(i):e instanceof wl?this.urlUpdateStrategy==="eager"&&(i.extras.skipLocationChange||this.setBrowserUrl(this.createBrowserPath(i),i)):e instanceof pa?(this.commitTransition(i),this.urlUpdateStrategy==="deferred"&&!i.extras.skipLocationChange&&this.setBrowserUrl(this.createBrowserPath(i),i)):e instanceof ti&&!wE(e)?this.restoreHistory(i):e instanceof ds?this.restoreHistory(i,!0):e instanceof xi&&(this.lastSuccessfulId=e.id,this.currentPageId=this.browserPageId)}setBrowserUrl(e,i){let{extras:r,id:o}=i,{replaceUrl:s,state:a}=r;if(this.location.isCurrentPathEqualTo(e)||s){let c=this.browserPageId,l=b(b({},a),this.generateNgRouterState(o,c,i));this.location.replaceState(e,"",l)}else{let c=b(b({},a),this.generateNgRouterState(o,this.browserPageId+1,i));this.location.go(e,"",c)}}restoreHistory(e,i=!1){if(this.canceledNavigationResolution==="computed"){let r=this.browserPageId,o=this.currentPageId-r;o!==0?this.location.historyGo(o):this.getCurrentUrlTree()===e.finalUrl&&o===0&&(this.resetInternalState(e),this.resetUrlToCurrentUrlTree())}else this.canceledNavigationResolution==="replace"&&(i&&this.resetInternalState(e),this.resetUrlToCurrentUrlTree())}resetInternalState({finalUrl:e}){this.routerState=this.stateMemento.routerState,this.currentUrlTree=this.stateMemento.currentUrlTree,this.rawUrlTree=this.urlHandlingStrategy.merge(this.currentUrlTree,e??this.rawUrlTree)}resetUrlToCurrentUrlTree(){this.location.replaceState(this.urlSerializer.serialize(this.getRawUrlTree()),"",this.generateNgRouterState(this.lastSuccessfulId,this.currentPageId))}generateNgRouterState(e,i,r){return this.canceledNavigationResolution==="computed"?b({navigationId:e,\u0275routerPageId:i},this.routerUrlState(r)):b({navigationId:e},this.routerUrlState(r))}static \u0275fac=(()=>{let e;return function(r){return(e||(e=ct(t)))(r||t)}})();static \u0275prov=C({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();function ub(t,n){t.events.pipe(De(e=>e instanceof xi||e instanceof ti||e instanceof ds||e instanceof br),Q(e=>e instanceof xi||e instanceof br?0:(e instanceof ti?e.code===ln.Redirect||e.code===ln.SupersededByNewNavigation:!1)?2:1),De(e=>e!==2),ut(1)).subscribe(()=>{n()})}var Xe=(()=>{class t{get currentUrlTree(){return this.stateManager.getCurrentUrlTree()}get rawUrlTree(){return this.stateManager.getRawUrlTree()}disposed=!1;nonRouterCurrentEntryChangeSubscription;console=d(pm);stateManager=d(pf);options=d(wa,{optional:!0})||{};pendingTasks=d(mr);urlUpdateStrategy=this.options.urlUpdateStrategy||"deferred";navigationTransitions=d(WE);urlSerializer=d(ya);location=d(Gi);urlHandlingStrategy=d(hf);injector=d(He);_events=new I;get events(){return this._events}get routerState(){return this.stateManager.getRouterState()}navigated=!1;routeReuseStrategy=d(YE);injectorCleanup=d(qE,{optional:!0});onSameUrlNavigation=this.options.onSameUrlNavigation||"ignore";config=d(Rl,{optional:!0})?.flat()??[];componentInputBindingEnabled=!!d(mf,{optional:!0});currentNavigation=this.navigationTransitions.currentNavigation.asReadonly();constructor(){this.resetConfig(this.config),this.navigationTransitions.setupNavigations(this).subscribe({error:e=>{}}),this.subscribeToNavigationEvents()}eventsSubscription=new ge;subscribeToNavigationEvents(){let e=this.navigationTransitions.events.subscribe(i=>{try{let r=this.navigationTransitions.currentTransition,o=Ne(this.navigationTransitions.currentNavigation);if(r!==null&&o!==null){if(this.stateManager.handleRouterEvent(i,o),i instanceof ti&&i.code!==ln.Redirect&&i.code!==ln.SupersededByNewNavigation)this.navigated=!0;else if(i instanceof xi)this.navigated=!0,this.injectorCleanup?.(this.routeReuseStrategy,this.routerState,this.config);else if(i instanceof ga){let s=i.navigationBehaviorOptions,a=this.urlHandlingStrategy.merge(i.url,r.currentRawUrl),c=b({scroll:r.extras.scroll,browserUrl:r.extras.browserUrl,info:r.extras.info,skipLocationChange:r.extras.skipLocationChange,replaceUrl:r.extras.replaceUrl||this.urlUpdateStrategy==="eager"||PF(r.source)},s);this.scheduleNavigation(a,vl,null,c,{resolve:r.resolve,reject:r.reject,promise:r.promise})}}LP(i)&&this._events.next(i)}catch(r){this.navigationTransitions.transitionAbortWithErrorSubject.next(r)}});this.eventsSubscription.add(e)}resetRootComponentType(e){this.routerState.root.component=e,this.navigationTransitions.rootComponentType=e}initialNavigation(){this.setUpLocationChangeListener(),this.navigationTransitions.hasRequestedNavigation||this.navigateToSyncWithBrowser(this.location.path(!0),vl,this.stateManager.restoredState(),{replaceUrl:!0})}setUpLocationChangeListener(){this.nonRouterCurrentEntryChangeSubscription??=this.stateManager.registerNonRouterCurrentEntryChangeListener((e,i,r,o)=>{this.navigateToSyncWithBrowser(e,r,i,o)})}navigateToSyncWithBrowser(e,i,r,o){let s=r?.navigationId?r:null,a=r?.\u0275routerUrl??e;if(r?.\u0275routerUrl&&(o=Z(b({},o),{browserUrl:e})),r){let l=b({},r);delete l.navigationId,delete l.\u0275routerPageId,delete l.\u0275routerUrl,Object.keys(l).length!==0&&(o.state=l)}let c=this.parseUrl(a);this.scheduleNavigation(c,i,s,o).catch(l=>{this.disposed||this.injector.get(Fn)(l)})}get url(){return this.serializeUrl(this.currentUrlTree)}getCurrentNavigation(){return Ne(this.navigationTransitions.currentNavigation)}get lastSuccessfulNavigation(){return this.navigationTransitions.lastSuccessfulNavigation}resetConfig(e){this.config=e.map(lb),this.navigated=!1}ngOnDestroy(){this.dispose()}dispose(){this._events.unsubscribe(),this.navigationTransitions.complete(),this.nonRouterCurrentEntryChangeSubscription?.unsubscribe(),this.nonRouterCurrentEntryChangeSubscription=void 0,this.disposed=!0,this.eventsSubscription.unsubscribe()}createUrlTree(e,i={}){let{relativeTo:r,queryParams:o,fragment:s,queryParamsHandling:a,preserveFragment:c}=i,l=c?this.currentUrlTree.fragment:s,u=null;switch(a??this.options.defaultQueryParamsHandling){case"merge":u=b(b({},this.currentUrlTree.queryParams),o);break;case"preserve":u=this.currentUrlTree.queryParams;break;default:u=o||null}u!==null&&(u=this.removeEmptyProps(u));let m;try{let g=r?r.snapshot:this.routerState.snapshot.root;m=bE(g)}catch{(typeof e[0]!="string"||e[0][0]!=="/")&&(e=[]),m=this.currentUrlTree.root}return yE(m,e,u,l??null,this.urlSerializer)}navigateByUrl(e,i={skipLocationChange:!1}){let r=ro(e)?e:this.parseUrl(e),o=this.urlHandlingStrategy.merge(r,this.rawUrlTree);return this.scheduleNavigation(o,vl,null,i)}navigate(e,i={skipLocationChange:!1}){return jF(e),this.navigateByUrl(this.createUrlTree(e,i),i)}serializeUrl(e){return this.urlSerializer.serialize(e)}parseUrl(e){try{return this.urlSerializer.parse(e)}catch{return this.console.warn(Oi(4018,!1)),this.urlSerializer.parse("/")}}isActive(e,i){let r;if(i===!0?r=b({},rb):i===!1?r=b({},yl):r=b(b({},yl),i),ro(e))return Gv(this.currentUrlTree,e,r);let o=this.parseUrl(e);return Gv(this.currentUrlTree,o,r)}removeEmptyProps(e){return Object.entries(e).reduce((i,[r,o])=>(o!=null&&(i[r]=o),i),{})}scheduleNavigation(e,i,r,o,s){if(this.disposed)return Promise.resolve(!1);let a,c,l;s?(a=s.resolve,c=s.reject,l=s.promise):l=new Promise((m,g)=>{a=m,c=g});let u=this.pendingTasks.add();return ub(this,()=>{queueMicrotask(()=>this.pendingTasks.remove(u))}),this.navigationTransitions.handleNavigationRequest({source:i,restoredState:r,currentUrlTree:this.currentUrlTree,currentRawUrl:this.currentUrlTree,rawUrl:e,extras:o,resolve:a,reject:c,promise:l,currentSnapshot:this.routerState.snapshot,currentRouterState:this.routerState}),l.catch(Promise.reject.bind(Promise))}static \u0275fac=function(i){return new(i||t)};static \u0275prov=C({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();function jF(t){for(let n=0;n<t.length;n++)if(t[n]==null)throw new R(4008,!1)}var VF=(()=>{class t{router=d(Xe);stateManager=d(pf);fragment=M("");queryParams=M({});path=M("");serializer=d(ya);constructor(){this.updateState(),this.router.events?.subscribe(e=>{e instanceof xi&&this.updateState()})}updateState(){let{fragment:e,root:i,queryParams:r}=this.stateManager.getCurrentUrlTree();this.fragment.set(e),this.queryParams.set(r),this.path.set(this.serializer.serialize(new bn(i)))}static \u0275fac=function(i){return new(i||t)};static \u0275prov=C({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})(),dt=(()=>{class t{router;route;tabIndexAttribute;renderer;el;locationStrategy;hrefAttributeValue=d(new Kn("href"),{optional:!0});reactiveHref=uv(()=>this.isAnchorElement?this.computeHref(this._urlTree()):this.hrefAttributeValue);get href(){return Ne(this.reactiveHref)}set href(e){this.reactiveHref.set(e)}set target(e){this._target.set(e)}get target(){return Ne(this._target)}_target=M(void 0);set queryParams(e){this._queryParams.set(e)}get queryParams(){return Ne(this._queryParams)}_queryParams=M(void 0,{equal:()=>!1});set fragment(e){this._fragment.set(e)}get fragment(){return Ne(this._fragment)}_fragment=M(void 0);set queryParamsHandling(e){this._queryParamsHandling.set(e)}get queryParamsHandling(){return Ne(this._queryParamsHandling)}_queryParamsHandling=M(void 0);set state(e){this._state.set(e)}get state(){return Ne(this._state)}_state=M(void 0,{equal:()=>!1});set info(e){this._info.set(e)}get info(){return Ne(this._info)}_info=M(void 0,{equal:()=>!1});set relativeTo(e){this._relativeTo.set(e)}get relativeTo(){return Ne(this._relativeTo)}_relativeTo=M(void 0);set preserveFragment(e){this._preserveFragment.set(e)}get preserveFragment(){return Ne(this._preserveFragment)}_preserveFragment=M(!1);set skipLocationChange(e){this._skipLocationChange.set(e)}get skipLocationChange(){return Ne(this._skipLocationChange)}_skipLocationChange=M(!1);set replaceUrl(e){this._replaceUrl.set(e)}get replaceUrl(){return Ne(this._replaceUrl)}_replaceUrl=M(!1);isAnchorElement;onChanges=new I;applicationErrorHandler=d(Fn);options=d(wa,{optional:!0});reactiveRouterState=d(VF);constructor(e,i,r,o,s,a){this.router=e,this.route=i,this.tabIndexAttribute=r,this.renderer=o,this.el=s,this.locationStrategy=a;let c=s.nativeElement.tagName?.toLowerCase();this.isAnchorElement=c==="a"||c==="area"||!!(typeof customElements=="object"&&customElements.get(c)?.observedAttributes?.includes?.("href"))}setTabIndexIfNotOnNativeEl(e){this.tabIndexAttribute!=null||this.isAnchorElement||this.applyAttributeValue("tabindex",e)}ngOnChanges(e){this.onChanges.next(this)}routerLinkInput=M(null);set routerLink(e){e==null?(this.routerLinkInput.set(null),this.setTabIndexIfNotOnNativeEl(null)):(ro(e)?this.routerLinkInput.set(e):this.routerLinkInput.set(Array.isArray(e)?e:[e]),this.setTabIndexIfNotOnNativeEl("0"))}onClick(e,i,r,o,s){let a=this._urlTree();if(a===null||this.isAnchorElement&&(e!==0||i||r||o||s||typeof this.target=="string"&&this.target!="_self"))return!0;let c={skipLocationChange:this.skipLocationChange,replaceUrl:this.replaceUrl,state:this.state,info:this.info};return this.router.navigateByUrl(a,c)?.catch(l=>{this.applicationErrorHandler(l)}),!this.isAnchorElement}ngOnDestroy(){}applyAttributeValue(e,i){let r=this.renderer,o=this.el.nativeElement;i!==null?r.setAttribute(o,e,i):r.removeAttribute(o,e)}_urlTree=xt(()=>{this.reactiveRouterState.path(),this._preserveFragment()&&this.reactiveRouterState.fragment();let e=r=>r==="preserve"||r==="merge";(e(this._queryParamsHandling())||e(this.options?.defaultQueryParamsHandling))&&this.reactiveRouterState.queryParams();let i=this.routerLinkInput();return i===null||!this.router.createUrlTree?null:ro(i)?i:this.router.createUrlTree(i,{relativeTo:this._relativeTo()!==void 0?this._relativeTo():this.route,queryParams:this._queryParams(),fragment:this._fragment(),queryParamsHandling:this._queryParamsHandling(),preserveFragment:this._preserveFragment()})},{equal:(e,i)=>this.computeHref(e)===this.computeHref(i)});get urlTree(){return Ne(this._urlTree)}computeHref(e){return e!==null&&this.locationStrategy?this.locationStrategy?.prepareExternalUrl(this.router.serializeUrl(e))??"":null}static \u0275fac=function(i){return new(i||t)(ee(Xe),ee(yn),$c("tabindex"),ee(xe),ee(L),ee(ca))};static \u0275dir=A({type:t,selectors:[["","routerLink",""]],hostVars:2,hostBindings:function(i,r){i&1&&T("click",function(s){return r.onClick(s.button,s.ctrlKey,s.shiftKey,s.altKey,s.metaKey)}),i&2&&q("href",r.reactiveHref(),M_)("target",r._target())},inputs:{target:"target",queryParams:"queryParams",fragment:"fragment",queryParamsHandling:"queryParamsHandling",state:"state",info:"info",relativeTo:"relativeTo",preserveFragment:[2,"preserveFragment","preserveFragment",B],skipLocationChange:[2,"skipLocationChange","skipLocationChange",B],replaceUrl:[2,"replaceUrl","replaceUrl",B],routerLink:"routerLink"},features:[ze]})}return t})(),mb=(()=>{class t{router;element;renderer;cdr;links;classes=[];routerEventsSubscription;linkInputChangesSubscription;_isActive=!1;get isActive(){return this._isActive}routerLinkActiveOptions={exact:!1};ariaCurrentWhenActive;isActiveChange=new U;link=d(dt,{optional:!0});constructor(e,i,r,o){this.router=e,this.element=i,this.renderer=r,this.cdr=o,this.routerEventsSubscription=e.events.subscribe(s=>{s instanceof xi&&this.update()})}ngAfterContentInit(){W(this.links.changes,W(null)).pipe(Fr()).subscribe(e=>{this.update(),this.subscribeToEachLinkOnChanges()})}subscribeToEachLinkOnChanges(){this.linkInputChangesSubscription?.unsubscribe();let e=[...this.links.toArray(),this.link].filter(i=>!!i).map(i=>i.onChanges);this.linkInputChangesSubscription=ot(e).pipe(Fr()).subscribe(i=>{this._isActive!==this.isLinkActive(this.router)(i)&&this.update()})}set routerLinkActive(e){let i=Array.isArray(e)?e:e.split(" ");this.classes=i.filter(r=>!!r)}ngOnChanges(e){this.update()}ngOnDestroy(){this.routerEventsSubscription.unsubscribe(),this.linkInputChangesSubscription?.unsubscribe()}update(){!this.links||!this.router.navigated||queueMicrotask(()=>{let e=this.hasActiveLinks();this.classes.forEach(i=>{e?this.renderer.addClass(this.element.nativeElement,i):this.renderer.removeClass(this.element.nativeElement,i)}),e&&this.ariaCurrentWhenActive!==void 0?this.renderer.setAttribute(this.element.nativeElement,"aria-current",this.ariaCurrentWhenActive.toString()):this.renderer.removeAttribute(this.element.nativeElement,"aria-current"),this._isActive!==e&&(this._isActive=e,this.cdr.markForCheck(),this.isActiveChange.emit(e))})}isLinkActive(e){let i=HF(this.routerLinkActiveOptions)?this.routerLinkActiveOptions:this.routerLinkActiveOptions.exact??!1?b({},rb):b({},yl);return r=>{let o=r.urlTree;return o?Ne(ob(o,e,i)):!1}}hasActiveLinks(){let e=this.isLinkActive(this.router);return this.link&&e(this.link)||this.links.some(e)}static \u0275fac=function(i){return new(i||t)(ee(Xe),ee(L),ee(xe),ee(ve))};static \u0275dir=A({type:t,selectors:[["","routerLinkActive",""]],contentQueries:function(i,r,o){if(i&1&&yt(o,dt,5),i&2){let s;$(s=G())&&(r.links=s)}},inputs:{routerLinkActiveOptions:"routerLinkActiveOptions",ariaCurrentWhenActive:"ariaCurrentWhenActive",routerLinkActive:"routerLinkActive"},outputs:{isActiveChange:"isActiveChange"},exportAs:["routerLinkActive"],features:[ze]})}return t})();function HF(t){let n=t;return!!(n.paths||n.matrixParams||n.queryParams||n.fragment)}var UF=new y("");function fb(t,...n){return Ni([{provide:Rl,multi:!0,useValue:t},[],{provide:yn,useFactory:zF},{provide:_m,multi:!0,useFactory:$F},n.map(e=>e.\u0275providers)])}function zF(){return d(Xe).routerState.root}function $F(){let t=d(X);return n=>{let e=t.get(Sn);if(n!==e.components[0])return;let i=t.get(Xe),r=t.get(GF);t.get(WF)===1&&i.initialNavigation(),t.get(qF,null,{optional:!0})?.setUpPreloading(),t.get(UF,null,{optional:!0})?.init(),i.resetRootComponentType(e.componentTypes[0]),r.closed||(r.next(),r.complete(),r.unsubscribe())}}var GF=new y("",{factory:()=>new I}),WF=new y("",{factory:()=>1});var qF=new y("");var gf=class t{router=d(Xe);ngOnInit(){this.router.navigateByUrl("/shop")}static \u0275fac=function(e){return new(e||t)};static \u0275cmp=E({type:t,selectors:[["app-home"]],decls:0,vars:0,template:function(e,i){},encapsulation:2})};var oo={production:!0,apiUrl:"/api/",hubUrl:"/hub/notifications",stripePublicKey:"pk_test_51ToQClBmCizU7eMNk0izYnpBZPzrrO1XDS4y3SQLTb6cjS7Au6vSIGyAufGv7R6TPZMXM31FSdyt0Kojc1XmuzS100zEbxJe8c"};var so=class t{baseUrl=oo.apiUrl;http=d(Wt);genres=[];getProducts(n){let e=new jn;return n.genres&&n.genres.length>0&&(e=e.append("genres",n.genres.join(","))),n.sort&&(e=e.append("sort",n.sort)),n.search&&(e=e.append("search",n.search)),e=e.append("pageSize",n.pageSize.toString()),e=e.append("pageIndex",n.pageIndex.toString()),this.http.get(this.baseUrl+"products",{params:e})}getProduct(n){return this.http.get(this.baseUrl+"products/"+n)}getGenres(){if(!(this.genres.length>0))return this.http.get(this.baseUrl+"products/genres").subscribe({next:n=>this.genres=n,error:n=>console.error(n)})}static \u0275fac=function(e){return new(e||t)};static \u0275prov=C({token:t,factory:t.\u0275fac,providedIn:"root"})};var ZF=new y("cdk-dir-doc",{providedIn:"root",factory:()=>d(K)}),QF=/^(ar|ckb|dv|he|iw|fa|nqo|ps|sd|ug|ur|yi|.*[-_](Adlm|Arab|Hebr|Nkoo|Rohg|Thaa))(?!.*[-_](Latn|Cyrl)($|-|_))($|-|_)/i;function ZE(t){let n=t?.toLowerCase()||"";return n==="auto"&&typeof navigator<"u"&&navigator?.language?QF.test(navigator.language)?"rtl":"ltr":n==="rtl"?"rtl":"ltr"}var Xt=(()=>{class t{get value(){return this.valueSignal()}valueSignal=M("ltr");change=new U;constructor(){let e=d(ZF,{optional:!0});if(e){let i=e.body?e.body.dir:null,r=e.documentElement?e.documentElement.dir:null;this.valueSignal.set(ZE(i||r||"ltr"))}}ngOnDestroy(){this.change.complete()}static \u0275fac=function(i){return new(i||t)};static \u0275prov=C({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();var Yt=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=ae({type:t});static \u0275inj=oe({})}return t})();var XF=["*"];var KF=new y("MAT_CARD_CONFIG"),Zi=(()=>{class t{appearance;constructor(){let e=d(KF,{optional:!0});this.appearance=e?.appearance||"raised"}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=E({type:t,selectors:[["mat-card"]],hostAttrs:[1,"mat-mdc-card","mdc-card"],hostVars:8,hostBindings:function(i,r){i&2&&z("mat-mdc-card-outlined",r.appearance==="outlined")("mdc-card--outlined",r.appearance==="outlined")("mat-mdc-card-filled",r.appearance==="filled")("mdc-card--filled",r.appearance==="filled")},inputs:{appearance:"appearance"},exportAs:["matCard"],ngContentSelectors:XF,decls:1,vars:0,template:function(i,r){i&1&&(Ce(),re(0))},styles:[`.mat-mdc-card {
  display: flex;
  flex-direction: column;
  box-sizing: border-box;
  position: relative;
  border-style: solid;
  border-width: 0;
  background-color: var(--mat-card-elevated-container-color, var(--mat-sys-surface-container-low));
  border-color: var(--mat-card-elevated-container-color, var(--mat-sys-surface-container-low));
  border-radius: var(--mat-card-elevated-container-shape, var(--mat-sys-corner-medium));
  box-shadow: var(--mat-card-elevated-container-elevation, var(--mat-sys-level1));
}
.mat-mdc-card::after {
  position: absolute;
  top: 0;
  left: 0;
  width: 100%;
  height: 100%;
  border: solid 1px transparent;
  content: "";
  display: block;
  pointer-events: none;
  box-sizing: border-box;
  border-radius: var(--mat-card-elevated-container-shape, var(--mat-sys-corner-medium));
}

.mat-mdc-card-outlined {
  background-color: var(--mat-card-outlined-container-color, var(--mat-sys-surface));
  border-radius: var(--mat-card-outlined-container-shape, var(--mat-sys-corner-medium));
  border-width: var(--mat-card-outlined-outline-width, 1px);
  border-color: var(--mat-card-outlined-outline-color, var(--mat-sys-outline-variant));
  box-shadow: var(--mat-card-outlined-container-elevation, var(--mat-sys-level0));
}
.mat-mdc-card-outlined::after {
  border: none;
}

.mat-mdc-card-filled {
  background-color: var(--mat-card-filled-container-color, var(--mat-sys-surface-container-highest));
  border-radius: var(--mat-card-filled-container-shape, var(--mat-sys-corner-medium));
  box-shadow: var(--mat-card-filled-container-elevation, var(--mat-sys-level0));
}

.mdc-card__media {
  position: relative;
  box-sizing: border-box;
  background-repeat: no-repeat;
  background-position: center;
  background-size: cover;
}
.mdc-card__media::before {
  display: block;
  content: "";
}
.mdc-card__media:first-child {
  border-top-left-radius: inherit;
  border-top-right-radius: inherit;
}
.mdc-card__media:last-child {
  border-bottom-left-radius: inherit;
  border-bottom-right-radius: inherit;
}

.mat-mdc-card-actions {
  display: flex;
  flex-direction: row;
  align-items: center;
  box-sizing: border-box;
  min-height: 52px;
  padding: 8px;
}

.mat-mdc-card-title {
  font-family: var(--mat-card-title-text-font, var(--mat-sys-title-large-font));
  line-height: var(--mat-card-title-text-line-height, var(--mat-sys-title-large-line-height));
  font-size: var(--mat-card-title-text-size, var(--mat-sys-title-large-size));
  letter-spacing: var(--mat-card-title-text-tracking, var(--mat-sys-title-large-tracking));
  font-weight: var(--mat-card-title-text-weight, var(--mat-sys-title-large-weight));
}

.mat-mdc-card-subtitle {
  color: var(--mat-card-subtitle-text-color, var(--mat-sys-on-surface));
  font-family: var(--mat-card-subtitle-text-font, var(--mat-sys-title-medium-font));
  line-height: var(--mat-card-subtitle-text-line-height, var(--mat-sys-title-medium-line-height));
  font-size: var(--mat-card-subtitle-text-size, var(--mat-sys-title-medium-size));
  letter-spacing: var(--mat-card-subtitle-text-tracking, var(--mat-sys-title-medium-tracking));
  font-weight: var(--mat-card-subtitle-text-weight, var(--mat-sys-title-medium-weight));
}

.mat-mdc-card-title,
.mat-mdc-card-subtitle {
  display: block;
  margin: 0;
}
.mat-mdc-card-avatar ~ .mat-mdc-card-header-text .mat-mdc-card-title,
.mat-mdc-card-avatar ~ .mat-mdc-card-header-text .mat-mdc-card-subtitle {
  padding: 16px 16px 0;
}

.mat-mdc-card-header {
  display: flex;
  padding: 16px 16px 0;
}

.mat-mdc-card-content {
  display: block;
  padding: 0 16px;
}
.mat-mdc-card-content:first-child {
  padding-top: 16px;
}
.mat-mdc-card-content:last-child {
  padding-bottom: 16px;
}

.mat-mdc-card-title-group {
  display: flex;
  justify-content: space-between;
  width: 100%;
}

.mat-mdc-card-avatar {
  height: 40px;
  width: 40px;
  border-radius: 50%;
  flex-shrink: 0;
  margin-bottom: 16px;
  object-fit: cover;
}
.mat-mdc-card-avatar ~ .mat-mdc-card-header-text .mat-mdc-card-subtitle,
.mat-mdc-card-avatar ~ .mat-mdc-card-header-text .mat-mdc-card-title {
  line-height: normal;
}

.mat-mdc-card-sm-image {
  width: 80px;
  height: 80px;
}

.mat-mdc-card-md-image {
  width: 112px;
  height: 112px;
}

.mat-mdc-card-lg-image {
  width: 152px;
  height: 152px;
}

.mat-mdc-card-xl-image {
  width: 240px;
  height: 240px;
}

.mat-mdc-card-subtitle ~ .mat-mdc-card-title,
.mat-mdc-card-title ~ .mat-mdc-card-subtitle,
.mat-mdc-card-header .mat-mdc-card-header-text .mat-mdc-card-title,
.mat-mdc-card-header .mat-mdc-card-header-text .mat-mdc-card-subtitle,
.mat-mdc-card-title-group .mat-mdc-card-title,
.mat-mdc-card-title-group .mat-mdc-card-subtitle {
  padding-top: 0;
}

.mat-mdc-card-content > :last-child:not(.mat-mdc-card-footer) {
  margin-bottom: 0;
}

.mat-mdc-card-actions-align-end {
  justify-content: flex-end;
}
`],encapsulation:2,changeDetection:0})}return t})();var QE=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275dir=A({type:t,selectors:[["mat-card-content"]],hostAttrs:[1,"mat-mdc-card-content"]})}return t})();var XE=(()=>{class t{align="start";static \u0275fac=function(i){return new(i||t)};static \u0275dir=A({type:t,selectors:[["mat-card-actions"]],hostAttrs:[1,"mat-mdc-card-actions","mdc-card__actions"],hostVars:2,hostBindings:function(i,r){i&2&&z("mat-mdc-card-actions-align-end",r.align==="end")},inputs:{align:"align"},exportAs:["matCardActions"]})}return t})();var KE=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=ae({type:t});static \u0275inj=oe({imports:[Yt]})}return t})();function ms(t){return t.buttons===0||t.detail===0}function fs(t){let n=t.touches&&t.touches[0]||t.changedTouches&&t.changedTouches[0];return!!n&&n.identifier===-1&&(n.radiusX==null||n.radiusX===1)&&(n.radiusY==null||n.radiusY===1)}var hb;function JE(){if(hb==null){let t=typeof document<"u"?document.head:null;hb=!!(t&&(t.createShadowRoot||t.attachShadow))}return hb}function pb(t){if(JE()){let n=t.getRootNode?t.getRootNode():null;if(typeof ShadowRoot<"u"&&ShadowRoot&&n instanceof ShadowRoot)return n}return null}function yr(){let t=typeof document<"u"&&document?document.activeElement:null;for(;t&&t.shadowRoot;){let n=t.shadowRoot.activeElement;if(n===t)break;t=n}return t}function Kt(t){return t.composedPath?t.composedPath()[0]:t.target}var gb;try{gb=typeof Intl<"u"&&Intl.v8BreakIterator}catch{gb=!1}var ye=(()=>{class t{_platformId=d(es);isBrowser=this._platformId?ND(this._platformId):typeof document=="object"&&!!document;EDGE=this.isBrowser&&/(edge)/i.test(navigator.userAgent);TRIDENT=this.isBrowser&&/(msie|trident)/i.test(navigator.userAgent);BLINK=this.isBrowser&&!!(window.chrome||gb)&&typeof CSS<"u"&&!this.EDGE&&!this.TRIDENT;WEBKIT=this.isBrowser&&/AppleWebKit/i.test(navigator.userAgent)&&!this.BLINK&&!this.EDGE&&!this.TRIDENT;IOS=this.isBrowser&&/iPad|iPhone|iPod/.test(navigator.userAgent)&&!("MSStream"in window);FIREFOX=this.isBrowser&&/(firefox|minefield)/i.test(navigator.userAgent);ANDROID=this.isBrowser&&/android/i.test(navigator.userAgent)&&!this.TRIDENT;SAFARI=this.isBrowser&&/safari/i.test(navigator.userAgent)&&this.WEBKIT;constructor(){}static \u0275fac=function(i){return new(i||t)};static \u0275prov=C({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();var Ol;function eS(){if(Ol==null&&typeof window<"u")try{window.addEventListener("test",null,Object.defineProperty({},"passive",{get:()=>Ol=!0}))}finally{Ol=Ol||!1}return Ol}function Da(t){return eS()?t:!!t.capture}function Hn(t,n=0){return tS(t)?Number(t):arguments.length===2?n:0}function tS(t){return!isNaN(parseFloat(t))&&!isNaN(Number(t))}function xn(t){return t instanceof L?t.nativeElement:t}var nS=new y("cdk-input-modality-detector-options"),iS={ignoreKeys:[18,17,224,91,16]},rS=650,_b={passive:!0,capture:!0},oS=(()=>{class t{_platform=d(ye);_listenerCleanups;modalityDetected;modalityChanged;get mostRecentModality(){return this._modality.value}_mostRecentTarget=null;_modality=new Ft(null);_options;_lastTouchMs=0;_onKeydown=e=>{this._options?.ignoreKeys?.some(i=>i===e.keyCode)||(this._modality.next("keyboard"),this._mostRecentTarget=Kt(e))};_onMousedown=e=>{Date.now()-this._lastTouchMs<rS||(this._modality.next(ms(e)?"keyboard":"mouse"),this._mostRecentTarget=Kt(e))};_onTouchstart=e=>{if(fs(e)){this._modality.next("keyboard");return}this._lastTouchMs=Date.now(),this._modality.next("touch"),this._mostRecentTarget=Kt(e)};constructor(){let e=d(H),i=d(K),r=d(nS,{optional:!0});if(this._options=b(b({},iS),r),this.modalityDetected=this._modality.pipe(mc(1)),this.modalityChanged=this.modalityDetected.pipe(Xd()),this._platform.isBrowser){let o=d(Ot).createRenderer(null,null);this._listenerCleanups=e.runOutsideAngular(()=>[o.listen(i,"keydown",this._onKeydown,_b),o.listen(i,"mousedown",this._onMousedown,_b),o.listen(i,"touchstart",this._onTouchstart,_b)])}}ngOnDestroy(){this._modality.complete(),this._listenerCleanups?.forEach(e=>e())}static \u0275fac=function(i){return new(i||t)};static \u0275prov=C({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})(),Nl=(function(t){return t[t.IMMEDIATE=0]="IMMEDIATE",t[t.EVENTUAL=1]="EVENTUAL",t})(Nl||{}),sS=new y("cdk-focus-monitor-default-options"),_f=Da({passive:!0,capture:!0}),Tn=(()=>{class t{_ngZone=d(H);_platform=d(ye);_inputModalityDetector=d(oS);_origin=null;_lastFocusOrigin=null;_windowFocused=!1;_windowFocusTimeoutId;_originTimeoutId;_originFromTouchInteraction=!1;_elementInfo=new Map;_monitoredElementCount=0;_rootNodeFocusListenerCount=new Map;_detectionMode;_windowFocusListener=()=>{this._windowFocused=!0,this._windowFocusTimeoutId=setTimeout(()=>this._windowFocused=!1)};_document=d(K);_stopInputModalityDetector=new I;constructor(){let e=d(sS,{optional:!0});this._detectionMode=e?.detectionMode||Nl.IMMEDIATE}_rootNodeFocusAndBlurListener=e=>{let i=Kt(e);for(let r=i;r;r=r.parentElement)e.type==="focus"?this._onFocus(e,r):this._onBlur(e,r)};monitor(e,i=!1){let r=xn(e);if(!this._platform.isBrowser||r.nodeType!==1)return W();let o=pb(r)||this._document,s=this._elementInfo.get(r);if(s)return i&&(s.checkChildren=!0),s.subject;let a={checkChildren:i,subject:new I,rootNode:o};return this._elementInfo.set(r,a),this._registerGlobalListeners(a),a.subject}stopMonitoring(e){let i=xn(e),r=this._elementInfo.get(i);r&&(r.subject.complete(),this._setClasses(i),this._elementInfo.delete(i),this._removeGlobalListeners(r))}focusVia(e,i,r){let o=xn(e),s=this._document.activeElement;o===s?this._getClosestElementsInfo(o).forEach(([a,c])=>this._originChanged(a,i,c)):(this._setOrigin(i),typeof o.focus=="function"&&o.focus(r))}ngOnDestroy(){this._elementInfo.forEach((e,i)=>this.stopMonitoring(i))}_getWindow(){return this._document.defaultView||window}_getFocusOrigin(e){return this._origin?this._originFromTouchInteraction?this._shouldBeAttributedToTouch(e)?"touch":"program":this._origin:this._windowFocused&&this._lastFocusOrigin?this._lastFocusOrigin:e&&this._isLastInteractionFromInputLabel(e)?"mouse":"program"}_shouldBeAttributedToTouch(e){return this._detectionMode===Nl.EVENTUAL||!!e?.contains(this._inputModalityDetector._mostRecentTarget)}_setClasses(e,i){e.classList.toggle("cdk-focused",!!i),e.classList.toggle("cdk-touch-focused",i==="touch"),e.classList.toggle("cdk-keyboard-focused",i==="keyboard"),e.classList.toggle("cdk-mouse-focused",i==="mouse"),e.classList.toggle("cdk-program-focused",i==="program")}_setOrigin(e,i=!1){this._ngZone.runOutsideAngular(()=>{if(this._origin=e,this._originFromTouchInteraction=e==="touch"&&i,this._detectionMode===Nl.IMMEDIATE){clearTimeout(this._originTimeoutId);let r=this._originFromTouchInteraction?rS:1;this._originTimeoutId=setTimeout(()=>this._origin=null,r)}})}_onFocus(e,i){let r=this._elementInfo.get(i),o=Kt(e);!r||!r.checkChildren&&i!==o||this._originChanged(i,this._getFocusOrigin(o),r)}_onBlur(e,i){let r=this._elementInfo.get(i);!r||r.checkChildren&&e.relatedTarget instanceof Node&&i.contains(e.relatedTarget)||(this._setClasses(i),this._emitOrigin(r,null))}_emitOrigin(e,i){e.subject.observers.length&&this._ngZone.run(()=>e.subject.next(i))}_registerGlobalListeners(e){if(!this._platform.isBrowser)return;let i=e.rootNode,r=this._rootNodeFocusListenerCount.get(i)||0;r||this._ngZone.runOutsideAngular(()=>{i.addEventListener("focus",this._rootNodeFocusAndBlurListener,_f),i.addEventListener("blur",this._rootNodeFocusAndBlurListener,_f)}),this._rootNodeFocusListenerCount.set(i,r+1),++this._monitoredElementCount===1&&(this._ngZone.runOutsideAngular(()=>{this._getWindow().addEventListener("focus",this._windowFocusListener)}),this._inputModalityDetector.modalityDetected.pipe(me(this._stopInputModalityDetector)).subscribe(o=>{this._setOrigin(o,!0)}))}_removeGlobalListeners(e){let i=e.rootNode;if(this._rootNodeFocusListenerCount.has(i)){let r=this._rootNodeFocusListenerCount.get(i);r>1?this._rootNodeFocusListenerCount.set(i,r-1):(i.removeEventListener("focus",this._rootNodeFocusAndBlurListener,_f),i.removeEventListener("blur",this._rootNodeFocusAndBlurListener,_f),this._rootNodeFocusListenerCount.delete(i))}--this._monitoredElementCount||(this._getWindow().removeEventListener("focus",this._windowFocusListener),this._stopInputModalityDetector.next(),clearTimeout(this._windowFocusTimeoutId),clearTimeout(this._originTimeoutId))}_originChanged(e,i,r){this._setClasses(e,i),this._emitOrigin(r,i),this._lastFocusOrigin=i}_getClosestElementsInfo(e){let i=[];return this._elementInfo.forEach((r,o)=>{(o===e||r.checkChildren&&o.contains(e))&&i.push([o,r])}),i}_isLastInteractionFromInputLabel(e){let{_mostRecentTarget:i,mostRecentModality:r}=this._inputModalityDetector;if(r!=="mouse"||!i||i===e||e.nodeName!=="INPUT"&&e.nodeName!=="TEXTAREA"||e.disabled)return!1;let o=e.labels;if(o){for(let s=0;s<o.length;s++)if(o[s].contains(i))return!0}return!1}static \u0275fac=function(i){return new(i||t)};static \u0275prov=C({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();var vf=new WeakMap,qe=(()=>{class t{_appRef;_injector=d(X);_environmentInjector=d(He);load(e){let i=this._appRef=this._appRef||this._injector.get(Sn),r=vf.get(i);r||(r={loaders:new Set,refs:[]},vf.set(i,r),i.onDestroy(()=>{vf.get(i)?.refs.forEach(o=>o.destroy()),vf.delete(i)})),r.loaders.has(e)||(r.loaders.add(e),r.refs.push(Dm(e,{environmentInjector:this._environmentInjector})))}static \u0275fac=function(i){return new(i||t)};static \u0275prov=C({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();var Qi=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275cmp=E({type:t,selectors:[["ng-component"]],exportAs:["cdkVisuallyHidden"],decls:0,vars:0,template:function(i,r){},styles:[`.cdk-visually-hidden {
  border: 0;
  clip: rect(0 0 0 0);
  height: 1px;
  margin: -1px;
  overflow: hidden;
  padding: 0;
  position: absolute;
  width: 1px;
  white-space: nowrap;
  outline: 0;
  -webkit-appearance: none;
  -moz-appearance: none;
  left: 0;
}
[dir=rtl] .cdk-visually-hidden {
  left: auto;
  right: 0;
}
`],encapsulation:2,changeDetection:0})}return t})(),bf;function eL(){if(bf===void 0&&(bf=null,typeof window<"u")){let t=window;t.trustedTypes!==void 0&&(bf=t.trustedTypes.createPolicy("angular#components",{createHTML:n=>n}))}return bf}function hs(t){return eL()?.createHTML(t)||t}function aS(t,n,e){let i=e.sanitize(Bt.HTML,n);t.innerHTML=hs(i||"")}function Ea(t){return Array.isArray(t)?t:[t]}var cS=new Set,ps,Sa=(()=>{class t{_platform=d(ye);_nonce=d(ts,{optional:!0});_matchMedia;constructor(){this._matchMedia=this._platform.isBrowser&&window.matchMedia?window.matchMedia.bind(window):nL}matchMedia(e){return(this._platform.WEBKIT||this._platform.BLINK)&&tL(e,this._nonce),this._matchMedia(e)}static \u0275fac=function(i){return new(i||t)};static \u0275prov=C({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();function tL(t,n){if(!cS.has(t))try{ps||(ps=document.createElement("style"),n&&ps.setAttribute("nonce",n),ps.setAttribute("type","text/css"),document.head.appendChild(ps)),ps.sheet&&(ps.sheet.insertRule(`@media ${t} {body{ }}`,0),cS.add(t))}catch(e){console.error(e)}}function nL(t){return{matches:t==="all"||t==="",media:t,addListener:()=>{},removeListener:()=>{}}}var vb=(()=>{class t{_mediaMatcher=d(Sa);_zone=d(H);_queries=new Map;_destroySubject=new I;constructor(){}ngOnDestroy(){this._destroySubject.next(),this._destroySubject.complete()}isMatched(e){return lS(Ea(e)).some(r=>this._registerQuery(r).mql.matches)}observe(e){let r=lS(Ea(e)).map(s=>this._registerQuery(s).observable),o=dc(r);return o=Lr(o.pipe(ut(1)),o.pipe(mc(1),Po(0))),o.pipe(Q(s=>{let a={matches:!1,breakpoints:{}};return s.forEach(({matches:c,query:l})=>{a.matches=a.matches||c,a.breakpoints[l]=c}),a}))}_registerQuery(e){if(this._queries.has(e))return this._queries.get(e);let i=this._mediaMatcher.matchMedia(e),o={observable:new ce(s=>{let a=c=>this._zone.run(()=>s.next(c));return i.addListener(a),()=>{i.removeListener(a)}}).pipe(et(i),Q(({matches:s})=>({query:e,matches:s})),me(this._destroySubject)),mql:i};return this._queries.set(e,o),o}static \u0275fac=function(i){return new(i||t)};static \u0275prov=C({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();function lS(t){return t.map(n=>n.split(",")).reduce((n,e)=>n.concat(e)).map(n=>n.trim())}function iL(t){if(t.type==="characterData"&&t.target instanceof Comment)return!0;if(t.type==="childList"){for(let n=0;n<t.addedNodes.length;n++)if(!(t.addedNodes[n]instanceof Comment))return!1;for(let n=0;n<t.removedNodes.length;n++)if(!(t.removedNodes[n]instanceof Comment))return!1;return!0}return!1}var rL=(()=>{class t{create(e){return typeof MutationObserver>"u"?null:new MutationObserver(e)}static \u0275fac=function(i){return new(i||t)};static \u0275prov=C({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})(),oL=(()=>{class t{_mutationObserverFactory=d(rL);_observedElements=new Map;_ngZone=d(H);constructor(){}ngOnDestroy(){this._observedElements.forEach((e,i)=>this._cleanupObserver(i))}observe(e){let i=xn(e);return new ce(r=>{let s=this._observeElement(i).pipe(Q(a=>a.filter(c=>!iL(c))),De(a=>!!a.length)).subscribe(a=>{this._ngZone.run(()=>{r.next(a)})});return()=>{s.unsubscribe(),this._unobserveElement(i)}})}_observeElement(e){return this._ngZone.runOutsideAngular(()=>{if(this._observedElements.has(e))this._observedElements.get(e).count++;else{let i=new I,r=this._mutationObserverFactory.create(o=>i.next(o));r&&r.observe(e,{characterData:!0,childList:!0,subtree:!0}),this._observedElements.set(e,{observer:r,stream:i,count:1})}return this._observedElements.get(e).stream})}_unobserveElement(e){this._observedElements.has(e)&&(this._observedElements.get(e).count--,this._observedElements.get(e).count||this._cleanupObserver(e))}_cleanupObserver(e){if(this._observedElements.has(e)){let{observer:i,stream:r}=this._observedElements.get(e);i&&i.disconnect(),r.complete(),this._observedElements.delete(e)}}static \u0275fac=function(i){return new(i||t)};static \u0275prov=C({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})(),dS=(()=>{class t{_contentObserver=d(oL);_elementRef=d(L);event=new U;get disabled(){return this._disabled}set disabled(e){this._disabled=e,this._disabled?this._unsubscribe():this._subscribe()}_disabled=!1;get debounce(){return this._debounce}set debounce(e){this._debounce=Hn(e),this._subscribe()}_debounce;_currentSubscription=null;constructor(){}ngAfterContentInit(){!this._currentSubscription&&!this.disabled&&this._subscribe()}ngOnDestroy(){this._unsubscribe()}_subscribe(){this._unsubscribe();let e=this._contentObserver.observe(this._elementRef);this._currentSubscription=(this.debounce?e.pipe(Po(this.debounce)):e).subscribe(this.event)}_unsubscribe(){this._currentSubscription?.unsubscribe()}static \u0275fac=function(i){return new(i||t)};static \u0275dir=A({type:t,selectors:[["","cdkObserveContent",""]],inputs:{disabled:[2,"cdkObserveContentDisabled","disabled",B],debounce:"debounce"},outputs:{event:"cdkObserveContent"},exportAs:["cdkObserveContent"]})}return t})();var Ia=(()=>{class t{_platform=d(ye);constructor(){}isDisabled(e){return e.hasAttribute("disabled")}isVisible(e){return aL(e)&&getComputedStyle(e).visibility==="visible"}isTabbable(e){if(!this._platform.isBrowser)return!1;let i=sL(pL(e));if(i&&(uS(i)===-1||!this.isVisible(i)))return!1;let r=e.nodeName.toLowerCase(),o=uS(e);return e.hasAttribute("contenteditable")?o!==-1:r==="iframe"||r==="object"||this._platform.WEBKIT&&this._platform.IOS&&!fL(e)?!1:r==="audio"?e.hasAttribute("controls")?o!==-1:!1:r==="video"?o===-1?!1:o!==null?!0:this._platform.FIREFOX||e.hasAttribute("controls"):e.tabIndex>=0}isFocusable(e,i){return hL(e)&&!this.isDisabled(e)&&(i?.ignoreVisibility||this.isVisible(e))}static \u0275fac=function(i){return new(i||t)};static \u0275prov=C({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();function sL(t){try{return t.frameElement}catch{return null}}function aL(t){return!!(t.offsetWidth||t.offsetHeight||typeof t.getClientRects=="function"&&t.getClientRects().length)}function cL(t){let n=t.nodeName.toLowerCase();return n==="input"||n==="select"||n==="button"||n==="textarea"}function lL(t){return uL(t)&&t.type=="hidden"}function dL(t){return mL(t)&&t.hasAttribute("href")}function uL(t){return t.nodeName.toLowerCase()=="input"}function mL(t){return t.nodeName.toLowerCase()=="a"}function mS(t){if(!t.hasAttribute("tabindex")||t.tabIndex===void 0)return!1;let n=t.getAttribute("tabindex");return!!(n&&!isNaN(parseInt(n,10)))}function uS(t){if(!mS(t))return null;let n=parseInt(t.getAttribute("tabindex")||"",10);return isNaN(n)?-1:n}function fL(t){let n=t.nodeName.toLowerCase(),e=n==="input"&&t.type;return e==="text"||e==="password"||n==="select"||n==="textarea"}function hL(t){return lL(t)?!1:cL(t)||dL(t)||t.hasAttribute("contenteditable")||mS(t)}function pL(t){return t.ownerDocument&&t.ownerDocument.defaultView||window}var yf=class{_element;_checker;_ngZone;_document;_injector;_startAnchor=null;_endAnchor=null;_hasAttached=!1;startAnchorListener=()=>this.focusLastTabbableElement();endAnchorListener=()=>this.focusFirstTabbableElement();get enabled(){return this._enabled}set enabled(n){this._enabled=n,this._startAnchor&&this._endAnchor&&(this._toggleAnchorTabIndex(n,this._startAnchor),this._toggleAnchorTabIndex(n,this._endAnchor))}_enabled=!0;constructor(n,e,i,r,o=!1,s){this._element=n,this._checker=e,this._ngZone=i,this._document=r,this._injector=s,o||this.attachAnchors()}destroy(){let n=this._startAnchor,e=this._endAnchor;n&&(n.removeEventListener("focus",this.startAnchorListener),n.remove()),e&&(e.removeEventListener("focus",this.endAnchorListener),e.remove()),this._startAnchor=this._endAnchor=null,this._hasAttached=!1}attachAnchors(){return this._hasAttached?!0:(this._ngZone.runOutsideAngular(()=>{this._startAnchor||(this._startAnchor=this._createAnchor(),this._startAnchor.addEventListener("focus",this.startAnchorListener)),this._endAnchor||(this._endAnchor=this._createAnchor(),this._endAnchor.addEventListener("focus",this.endAnchorListener))}),this._element.parentNode&&(this._element.parentNode.insertBefore(this._startAnchor,this._element),this._element.parentNode.insertBefore(this._endAnchor,this._element.nextSibling),this._hasAttached=!0),this._hasAttached)}focusInitialElementWhenReady(n){return new Promise(e=>{this._executeOnStable(()=>e(this.focusInitialElement(n)))})}focusFirstTabbableElementWhenReady(n){return new Promise(e=>{this._executeOnStable(()=>e(this.focusFirstTabbableElement(n)))})}focusLastTabbableElementWhenReady(n){return new Promise(e=>{this._executeOnStable(()=>e(this.focusLastTabbableElement(n)))})}_getRegionBoundary(n){let e=this._element.querySelectorAll(`[cdk-focus-region-${n}], [cdkFocusRegion${n}], [cdk-focus-${n}]`);return n=="start"?e.length?e[0]:this._getFirstTabbableElement(this._element):e.length?e[e.length-1]:this._getLastTabbableElement(this._element)}focusInitialElement(n){let e=this._element.querySelector("[cdk-focus-initial], [cdkFocusInitial]");if(e){if(!this._checker.isFocusable(e)){let i=this._getFirstTabbableElement(e);return i?.focus(n),!!i}return e.focus(n),!0}return this.focusFirstTabbableElement(n)}focusFirstTabbableElement(n){let e=this._getRegionBoundary("start");return e&&e.focus(n),!!e}focusLastTabbableElement(n){let e=this._getRegionBoundary("end");return e&&e.focus(n),!!e}hasAttached(){return this._hasAttached}_getFirstTabbableElement(n){if(this._checker.isFocusable(n)&&this._checker.isTabbable(n))return n;let e=n.children;for(let i=0;i<e.length;i++){let r=e[i].nodeType===this._document.ELEMENT_NODE?this._getFirstTabbableElement(e[i]):null;if(r)return r}return null}_getLastTabbableElement(n){if(this._checker.isFocusable(n)&&this._checker.isTabbable(n))return n;let e=n.children;for(let i=e.length-1;i>=0;i--){let r=e[i].nodeType===this._document.ELEMENT_NODE?this._getLastTabbableElement(e[i]):null;if(r)return r}return null}_createAnchor(){let n=this._document.createElement("div");return this._toggleAnchorTabIndex(this._enabled,n),n.classList.add("cdk-visually-hidden"),n.classList.add("cdk-focus-trap-anchor"),n.setAttribute("aria-hidden","true"),n}_toggleAnchorTabIndex(n,e){n?e.setAttribute("tabindex","0"):e.removeAttribute("tabindex")}toggleAnchors(n){this._startAnchor&&this._endAnchor&&(this._toggleAnchorTabIndex(n,this._startAnchor),this._toggleAnchorTabIndex(n,this._endAnchor))}_executeOnStable(n){this._injector?bt(n,{injector:this._injector}):setTimeout(n)}},bb=(()=>{class t{_checker=d(Ia);_ngZone=d(H);_document=d(K);_injector=d(X);constructor(){d(qe).load(Qi)}create(e,i=!1){return new yf(e,this._checker,this._ngZone,this._document,i,this._injector)}static \u0275fac=function(i){return new(i||t)};static \u0275prov=C({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();var fS=new y("liveAnnouncerElement",{providedIn:"root",factory:()=>null}),hS=new y("LIVE_ANNOUNCER_DEFAULT_OPTIONS"),gL=0,Pl=(()=>{class t{_ngZone=d(H);_defaultOptions=d(hS,{optional:!0});_liveElement;_document=d(K);_sanitizer=d(hl);_previousTimeout;_currentPromise;_currentResolve;constructor(){let e=d(fS,{optional:!0});this._liveElement=e||this._createLiveElement()}announce(e,...i){let r=this._defaultOptions,o,s;return i.length===1&&typeof i[0]=="number"?s=i[0]:[o,s]=i,this.clear(),clearTimeout(this._previousTimeout),o||(o=r&&r.politeness?r.politeness:"polite"),s==null&&r&&(s=r.duration),this._liveElement.setAttribute("aria-live",o),this._liveElement.id&&this._exposeAnnouncerToModals(this._liveElement.id),this._ngZone.runOutsideAngular(()=>(this._currentPromise||(this._currentPromise=new Promise(a=>this._currentResolve=a)),clearTimeout(this._previousTimeout),this._previousTimeout=setTimeout(()=>{!e||typeof e=="string"?this._liveElement.textContent=e:aS(this._liveElement,e,this._sanitizer),typeof s=="number"&&(this._previousTimeout=setTimeout(()=>this.clear(),s)),this._currentResolve?.(),this._currentPromise=this._currentResolve=void 0},100),this._currentPromise))}clear(){this._liveElement&&(this._liveElement.textContent="")}ngOnDestroy(){clearTimeout(this._previousTimeout),this._liveElement?.remove(),this._liveElement=null,this._currentResolve?.(),this._currentPromise=this._currentResolve=void 0}_createLiveElement(){let e="cdk-live-announcer-element",i=this._document.getElementsByClassName(e),r=this._document.createElement("div");for(let o=0;o<i.length;o++)i[o].remove();return r.classList.add(e),r.classList.add("cdk-visually-hidden"),r.setAttribute("aria-atomic","true"),r.setAttribute("aria-live","polite"),r.id=`cdk-live-announcer-${gL++}`,this._document.body.appendChild(r),r}_exposeAnnouncerToModals(e){let i=this._document.querySelectorAll('body > .cdk-overlay-container [aria-modal="true"]');for(let r=0;r<i.length;r++){let o=i[r],s=o.getAttribute("aria-owns");s?s.indexOf(e)===-1&&o.setAttribute("aria-owns",s+" "+e):o.setAttribute("aria-owns",e)}}static \u0275fac=function(i){return new(i||t)};static \u0275prov=C({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();var _L=200,xf=class{_letterKeyStream=new I;_items=[];_selectedItemIndex=-1;_pressedLetters=[];_skipPredicateFn;_selectedItem=new I;selectedItem=this._selectedItem;constructor(n,e){let i=typeof e?.debounceInterval=="number"?e.debounceInterval:_L;e?.skipPredicate&&(this._skipPredicateFn=e.skipPredicate),this.setItems(n),this._setupKeyHandler(i)}destroy(){this._pressedLetters=[],this._letterKeyStream.complete(),this._selectedItem.complete()}setCurrentSelectedItemIndex(n){this._selectedItemIndex=n}setItems(n){this._items=n}handleKey(n){let e=n.keyCode;n.key&&n.key.length===1?this._letterKeyStream.next(n.key.toLocaleUpperCase()):(e>=65&&e<=90||e>=48&&e<=57)&&this._letterKeyStream.next(String.fromCharCode(e))}isTyping(){return this._pressedLetters.length>0}reset(){this._pressedLetters=[]}_setupKeyHandler(n){this._letterKeyStream.pipe(st(e=>this._pressedLetters.push(e)),Po(n),De(()=>this._pressedLetters.length>0),Q(()=>this._pressedLetters.join("").toLocaleUpperCase())).subscribe(e=>{for(let i=1;i<this._items.length+1;i++){let r=(this._selectedItemIndex+i)%this._items.length,o=this._items[r];if(!this._skipPredicateFn?.(o)&&o.getLabel?.().toLocaleUpperCase().trim().indexOf(e)===0){this._selectedItem.next(o);break}}this._pressedLetters=[]})}};function Ct(t,...n){return n.length?n.some(e=>t[e]):t.altKey||t.shiftKey||t.ctrlKey||t.metaKey}var ka=class{_items;_activeItemIndex=M(-1);_activeItem=M(null);_wrap=!1;_typeaheadSubscription=ge.EMPTY;_itemChangesSubscription;_vertical=!0;_horizontal=null;_allowedModifierKeys=[];_homeAndEnd=!1;_pageUpAndDown={enabled:!1,delta:10};_effectRef;_typeahead;_skipPredicateFn=n=>n.disabled;constructor(n,e){this._items=n,n instanceof En?this._itemChangesSubscription=n.changes.subscribe(i=>this._itemsChanged(i.toArray())):$i(n)&&(this._effectRef=fr(()=>this._itemsChanged(n()),{injector:e}))}tabOut=new I;change=new I;skipPredicate(n){return this._skipPredicateFn=n,this}withWrap(n=!0){return this._wrap=n,this}withVerticalOrientation(n=!0){return this._vertical=n,this}withHorizontalOrientation(n){return this._horizontal=n,this}withAllowedModifierKeys(n){return this._allowedModifierKeys=n,this}withTypeAhead(n=200){this._typeaheadSubscription.unsubscribe();let e=this._getItemsArray();return this._typeahead=new xf(e,{debounceInterval:typeof n=="number"?n:void 0,skipPredicate:i=>this._skipPredicateFn(i)}),this._typeaheadSubscription=this._typeahead.selectedItem.subscribe(i=>{this.setActiveItem(i)}),this}cancelTypeahead(){return this._typeahead?.reset(),this}withHomeAndEnd(n=!0){return this._homeAndEnd=n,this}withPageUpDown(n=!0,e=10){return this._pageUpAndDown={enabled:n,delta:e},this}setActiveItem(n){let e=this._activeItem();this.updateActiveItem(n),this._activeItem()!==e&&this.change.next(this._activeItemIndex())}onKeydown(n){let e=n.keyCode,r=["altKey","ctrlKey","metaKey","shiftKey"].every(o=>!n[o]||this._allowedModifierKeys.indexOf(o)>-1);switch(e){case 9:this.tabOut.next();return;case 40:if(this._vertical&&r){this.setNextItemActive();break}else return;case 38:if(this._vertical&&r){this.setPreviousItemActive();break}else return;case 39:if(this._horizontal&&r){this._horizontal==="rtl"?this.setPreviousItemActive():this.setNextItemActive();break}else return;case 37:if(this._horizontal&&r){this._horizontal==="rtl"?this.setNextItemActive():this.setPreviousItemActive();break}else return;case 36:if(this._homeAndEnd&&r){this.setFirstItemActive();break}else return;case 35:if(this._homeAndEnd&&r){this.setLastItemActive();break}else return;case 33:if(this._pageUpAndDown.enabled&&r){let o=this._activeItemIndex()-this._pageUpAndDown.delta;this._setActiveItemByIndex(o>0?o:0,1);break}else return;case 34:if(this._pageUpAndDown.enabled&&r){let o=this._activeItemIndex()+this._pageUpAndDown.delta,s=this._getItemsArray().length;this._setActiveItemByIndex(o<s?o:s-1,-1);break}else return;default:(r||Ct(n,"shiftKey"))&&this._typeahead?.handleKey(n);return}this._typeahead?.reset(),n.preventDefault()}get activeItemIndex(){return this._activeItemIndex()}get activeItem(){return this._activeItem()}isTyping(){return!!this._typeahead&&this._typeahead.isTyping()}setFirstItemActive(){this._setActiveItemByIndex(0,1)}setLastItemActive(){this._setActiveItemByIndex(this._getItemsArray().length-1,-1)}setNextItemActive(){this._activeItemIndex()<0?this.setFirstItemActive():this._setActiveItemByDelta(1)}setPreviousItemActive(){this._activeItemIndex()<0&&this._wrap?this.setLastItemActive():this._setActiveItemByDelta(-1)}updateActiveItem(n){let e=this._getItemsArray(),i=typeof n=="number"?n:e.indexOf(n),r=e[i];this._activeItem.set(r??null),this._activeItemIndex.set(i),this._typeahead?.setCurrentSelectedItemIndex(i)}destroy(){this._typeaheadSubscription.unsubscribe(),this._itemChangesSubscription?.unsubscribe(),this._effectRef?.destroy(),this._typeahead?.destroy(),this.tabOut.complete(),this.change.complete()}_setActiveItemByDelta(n){this._wrap?this._setActiveInWrapMode(n):this._setActiveInDefaultMode(n)}_setActiveInWrapMode(n){let e=this._getItemsArray();for(let i=1;i<=e.length;i++){let r=(this._activeItemIndex()+n*i+e.length)%e.length,o=e[r];if(!this._skipPredicateFn(o)){this.setActiveItem(r);return}}}_setActiveInDefaultMode(n){this._setActiveItemByIndex(this._activeItemIndex()+n,n)}_setActiveItemByIndex(n,e){let i=this._getItemsArray();if(i[n]){for(;this._skipPredicateFn(i[n]);)if(n+=e,!i[n])return;this.setActiveItem(n)}}_getItemsArray(){return $i(this._items)?this._items():this._items instanceof En?this._items.toArray():this._items}_itemsChanged(n){this._typeahead?.setItems(n);let e=this._activeItem();if(e){let i=n.indexOf(e);i>-1&&i!==this._activeItemIndex()&&(this._activeItemIndex.set(i),this._typeahead?.setCurrentSelectedItemIndex(i))}}};var Vl=class extends ka{setActiveItem(n){this.activeItem&&this.activeItem.setInactiveStyles(),super.setActiveItem(n),this.activeItem&&this.activeItem.setActiveStyles()}};var xr=class extends ka{_origin="program";setFocusOrigin(n){return this._origin=n,this}setActiveItem(n){super.setActiveItem(n),this.activeItem&&this.activeItem.focus(this._origin)}};var yb={},Ie=class t{_appId=d(Yr);static _infix=`a${Math.floor(Math.random()*1e5).toString()}`;getId(n,e=!1){return this._appId!=="ng"&&(n+=this._appId),yb.hasOwnProperty(n)||(yb[n]=0),`${n}${e?t._infix+"-":""}${yb[n]++}`}static \u0275fac=function(e){return new(e||t)};static \u0275prov=C({token:t,factory:t.\u0275fac,providedIn:"root"})};var _S=" ";function wb(t,n,e){let i=Df(t,n);e=e.trim(),!i.some(r=>r.trim()===e)&&(i.push(e),t.setAttribute(n,i.join(_S)))}function Ef(t,n,e){let i=Df(t,n);e=e.trim();let r=i.filter(o=>o!==e);r.length?t.setAttribute(n,r.join(_S)):t.removeAttribute(n)}function Df(t,n){return t.getAttribute(n)?.match(/\S+/g)??[]}var vS="cdk-describedby-message",wf="cdk-describedby-host",Cb=0,Sf=(()=>{class t{_platform=d(ye);_document=d(K);_messageRegistry=new Map;_messagesContainer=null;_id=`${Cb++}`;constructor(){d(qe).load(Qi),this._id=d(Yr)+"-"+Cb++}describe(e,i,r){if(!this._canBeDescribed(e,i))return;let o=xb(i,r);typeof i!="string"?(pS(i,this._id),this._messageRegistry.set(o,{messageElement:i,referenceCount:0})):this._messageRegistry.has(o)||this._createMessageElement(i,r),this._isElementDescribedByMessage(e,o)||this._addMessageReference(e,o)}removeDescription(e,i,r){if(!i||!this._isElementNode(e))return;let o=xb(i,r);if(this._isElementDescribedByMessage(e,o)&&this._removeMessageReference(e,o),typeof i=="string"){let s=this._messageRegistry.get(o);s&&s.referenceCount===0&&this._deleteMessageElement(o)}this._messagesContainer?.childNodes.length===0&&(this._messagesContainer.remove(),this._messagesContainer=null)}ngOnDestroy(){let e=this._document.querySelectorAll(`[${wf}="${this._id}"]`);for(let i=0;i<e.length;i++)this._removeCdkDescribedByReferenceIds(e[i]),e[i].removeAttribute(wf);this._messagesContainer?.remove(),this._messagesContainer=null,this._messageRegistry.clear()}_createMessageElement(e,i){let r=this._document.createElement("div");pS(r,this._id),r.textContent=e,i&&r.setAttribute("role",i),this._createMessagesContainer(),this._messagesContainer.appendChild(r),this._messageRegistry.set(xb(e,i),{messageElement:r,referenceCount:0})}_deleteMessageElement(e){this._messageRegistry.get(e)?.messageElement?.remove(),this._messageRegistry.delete(e)}_createMessagesContainer(){if(this._messagesContainer)return;let e="cdk-describedby-message-container",i=this._document.querySelectorAll(`.${e}[platform="server"]`);for(let o=0;o<i.length;o++)i[o].remove();let r=this._document.createElement("div");r.style.visibility="hidden",r.classList.add(e),r.classList.add("cdk-visually-hidden"),this._platform.isBrowser||r.setAttribute("platform","server"),this._document.body.appendChild(r),this._messagesContainer=r}_removeCdkDescribedByReferenceIds(e){let i=Df(e,"aria-describedby").filter(r=>r.indexOf(vS)!=0);e.setAttribute("aria-describedby",i.join(" "))}_addMessageReference(e,i){let r=this._messageRegistry.get(i);wb(e,"aria-describedby",r.messageElement.id),e.setAttribute(wf,this._id),r.referenceCount++}_removeMessageReference(e,i){let r=this._messageRegistry.get(i);r.referenceCount--,Ef(e,"aria-describedby",r.messageElement.id),e.removeAttribute(wf)}_isElementDescribedByMessage(e,i){let r=Df(e,"aria-describedby"),o=this._messageRegistry.get(i),s=o&&o.messageElement.id;return!!s&&r.indexOf(s)!=-1}_canBeDescribed(e,i){if(!this._isElementNode(e))return!1;if(i&&typeof i=="object")return!0;let r=i==null?"":`${i}`.trim(),o=e.getAttribute("aria-label");return r?!o||o.trim()!==r:!1}_isElementNode(e){return e.nodeType===this._document.ELEMENT_NODE}static \u0275fac=function(i){return new(i||t)};static \u0275prov=C({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();function xb(t,n){return typeof t=="string"?`${n||""}/${t}`:t}function pS(t,n){t.id||(t.id=`${vS}-${n}-${Cb++}`)}var gs;function bS(){if(gs==null){if(typeof document!="object"||!document||typeof Element!="function"||!Element)return gs=!1,gs;if(document.documentElement?.style&&"scrollBehavior"in document.documentElement.style)gs=!0;else{let t=Element.prototype.scrollTo;t?gs=!/\{\s*\[native code\]\s*\}/.test(t.toString()):gs=!1}}return gs}function Db(){return typeof __karma__<"u"&&!!__karma__||typeof jasmine<"u"&&!!jasmine||typeof jest<"u"&&!!jest||typeof Mocha<"u"&&!!Mocha}var Ma,yS=["color","button","checkbox","date","datetime-local","email","file","hidden","image","month","number","password","radio","range","reset","search","submit","tel","text","time","url","week"];function Eb(){if(Ma)return Ma;if(typeof document!="object"||!document)return Ma=new Set(yS),Ma;let t=document.createElement("input");return Ma=new Set(yS.filter(n=>(t.setAttribute("type",n),t.type===n))),Ma}var xS={XSmall:"(max-width: 599.98px)",Small:"(min-width: 600px) and (max-width: 959.98px)",Medium:"(min-width: 960px) and (max-width: 1279.98px)",Large:"(min-width: 1280px) and (max-width: 1919.98px)",XLarge:"(min-width: 1920px)",Handset:"(max-width: 599.98px) and (orientation: portrait), (max-width: 959.98px) and (orientation: landscape)",Tablet:"(min-width: 600px) and (max-width: 839.98px) and (orientation: portrait), (min-width: 960px) and (max-width: 1279.98px) and (orientation: landscape)",Web:"(min-width: 840px) and (orientation: portrait), (min-width: 1280px) and (orientation: landscape)",HandsetPortrait:"(max-width: 599.98px) and (orientation: portrait)",TabletPortrait:"(min-width: 600px) and (max-width: 839.98px) and (orientation: portrait)",WebPortrait:"(min-width: 840px) and (orientation: portrait)",HandsetLandscape:"(max-width: 959.98px) and (orientation: landscape)",TabletLandscape:"(min-width: 960px) and (max-width: 1279.98px) and (orientation: landscape)",WebLandscape:"(min-width: 1280px) and (orientation: landscape)"};var vL=new y("MATERIAL_ANIMATIONS"),CS=null;function Hl(){return d(vL,{optional:!0})?.animationsDisabled||d(Gc,{optional:!0})==="NoopAnimations"?"di-disabled":(CS??=d(Sa).matchMedia("(prefers-reduced-motion)").matches,CS?"reduced-motion":"enabled")}function ke(){return Hl()!=="enabled"}function At(t){return t==null?"":typeof t=="string"?t:`${t}px`}function Rt(t){return t!=null&&`${t}`!="false"}var ni=(function(t){return t[t.FADING_IN=0]="FADING_IN",t[t.VISIBLE=1]="VISIBLE",t[t.FADING_OUT=2]="FADING_OUT",t[t.HIDDEN=3]="HIDDEN",t})(ni||{}),Sb=class{_renderer;element;config;_animationForciblyDisabledThroughCss;state=ni.HIDDEN;constructor(n,e,i,r=!1){this._renderer=n,this.element=e,this.config=i,this._animationForciblyDisabledThroughCss=r}fadeOut(){this._renderer.fadeOutRipple(this)}},wS=Da({passive:!0,capture:!0}),Ib=class{_events=new Map;addHandler(n,e,i,r){let o=this._events.get(e);if(o){let s=o.get(i);s?s.add(r):o.set(i,new Set([r]))}else this._events.set(e,new Map([[i,new Set([r])]])),n.runOutsideAngular(()=>{document.addEventListener(e,this._delegateEventHandler,wS)})}removeHandler(n,e,i){let r=this._events.get(n);if(!r)return;let o=r.get(e);o&&(o.delete(i),o.size===0&&r.delete(e),r.size===0&&(this._events.delete(n),document.removeEventListener(n,this._delegateEventHandler,wS)))}_delegateEventHandler=n=>{let e=Kt(n);e&&this._events.get(n.type)?.forEach((i,r)=>{(r===e||r.contains(e))&&i.forEach(o=>o.handleEvent(n))})}},Ul={enterDuration:225,exitDuration:150},bL=800,DS=Da({passive:!0,capture:!0}),ES=["mousedown","touchstart"],SS=["mouseup","mouseleave","touchend","touchcancel"],yL=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275cmp=E({type:t,selectors:[["ng-component"]],hostAttrs:["mat-ripple-style-loader",""],decls:0,vars:0,template:function(i,r){},styles:[`.mat-ripple {
  overflow: hidden;
  position: relative;
}
.mat-ripple:not(:empty) {
  transform: translateZ(0);
}

.mat-ripple.mat-ripple-unbounded {
  overflow: visible;
}

.mat-ripple-element {
  position: absolute;
  border-radius: 50%;
  pointer-events: none;
  transition: opacity, transform 0ms cubic-bezier(0, 0, 0.2, 1);
  transform: scale3d(0, 0, 0);
  background-color: var(--mat-ripple-color, color-mix(in srgb, var(--mat-sys-on-surface) 10%, transparent));
}
@media (forced-colors: active) {
  .mat-ripple-element {
    display: none;
  }
}
.cdk-drag-preview .mat-ripple-element, .cdk-drag-placeholder .mat-ripple-element {
  display: none;
}
`],encapsulation:2,changeDetection:0})}return t})(),_s=class t{_target;_ngZone;_platform;_containerElement;_triggerElement=null;_isPointerDown=!1;_activeRipples=new Map;_mostRecentTransientRipple=null;_lastTouchStartEvent;_pointerUpEventsRegistered=!1;_containerRect=null;static _eventManager=new Ib;constructor(n,e,i,r,o){this._target=n,this._ngZone=e,this._platform=r,r.isBrowser&&(this._containerElement=xn(i)),o&&o.get(qe).load(yL)}fadeInRipple(n,e,i={}){let r=this._containerRect=this._containerRect||this._containerElement.getBoundingClientRect(),o=b(b({},Ul),i.animation);i.centered&&(n=r.left+r.width/2,e=r.top+r.height/2);let s=i.radius||xL(n,e,r),a=n-r.left,c=e-r.top,l=o.enterDuration,u=document.createElement("div");u.classList.add("mat-ripple-element"),u.style.left=`${a-s}px`,u.style.top=`${c-s}px`,u.style.height=`${s*2}px`,u.style.width=`${s*2}px`,i.color!=null&&(u.style.backgroundColor=i.color),u.style.transitionDuration=`${l}ms`,this._containerElement.appendChild(u);let m=window.getComputedStyle(u),g=m.transitionProperty,v=m.transitionDuration,x=g==="none"||v==="0s"||v==="0s, 0s"||r.width===0&&r.height===0,F=new Sb(this,u,i,x);u.style.transform="scale3d(1, 1, 1)",F.state=ni.FADING_IN,i.persistent||(this._mostRecentTransientRipple=F);let j=null;return!x&&(l||o.exitDuration)&&this._ngZone.runOutsideAngular(()=>{let Y=()=>{j&&(j.fallbackTimer=null),clearTimeout(Dt),this._finishRippleTransition(F)},be=()=>this._destroyRipple(F),Dt=setTimeout(be,l+100);u.addEventListener("transitionend",Y),u.addEventListener("transitioncancel",be),j={onTransitionEnd:Y,onTransitionCancel:be,fallbackTimer:Dt}}),this._activeRipples.set(F,j),(x||!l)&&this._finishRippleTransition(F),F}fadeOutRipple(n){if(n.state===ni.FADING_OUT||n.state===ni.HIDDEN)return;let e=n.element,i=b(b({},Ul),n.config.animation);e.style.transitionDuration=`${i.exitDuration}ms`,e.style.opacity="0",n.state=ni.FADING_OUT,(n._animationForciblyDisabledThroughCss||!i.exitDuration)&&this._finishRippleTransition(n)}fadeOutAll(){this._getActiveRipples().forEach(n=>n.fadeOut())}fadeOutAllNonPersistent(){this._getActiveRipples().forEach(n=>{n.config.persistent||n.fadeOut()})}setupTriggerEvents(n){let e=xn(n);!this._platform.isBrowser||!e||e===this._triggerElement||(this._removeTriggerEvents(),this._triggerElement=e,ES.forEach(i=>{t._eventManager.addHandler(this._ngZone,i,e,this)}))}handleEvent(n){n.type==="mousedown"?this._onMousedown(n):n.type==="touchstart"?this._onTouchStart(n):this._onPointerUp(),this._pointerUpEventsRegistered||(this._ngZone.runOutsideAngular(()=>{SS.forEach(e=>{this._triggerElement.addEventListener(e,this,DS)})}),this._pointerUpEventsRegistered=!0)}_finishRippleTransition(n){n.state===ni.FADING_IN?this._startFadeOutTransition(n):n.state===ni.FADING_OUT&&this._destroyRipple(n)}_startFadeOutTransition(n){let e=n===this._mostRecentTransientRipple,{persistent:i}=n.config;n.state=ni.VISIBLE,!i&&(!e||!this._isPointerDown)&&n.fadeOut()}_destroyRipple(n){let e=this._activeRipples.get(n)??null;this._activeRipples.delete(n),this._activeRipples.size||(this._containerRect=null),n===this._mostRecentTransientRipple&&(this._mostRecentTransientRipple=null),n.state=ni.HIDDEN,e!==null&&(n.element.removeEventListener("transitionend",e.onTransitionEnd),n.element.removeEventListener("transitioncancel",e.onTransitionCancel),e.fallbackTimer!==null&&clearTimeout(e.fallbackTimer)),n.element.remove()}_onMousedown(n){let e=ms(n),i=this._lastTouchStartEvent&&Date.now()<this._lastTouchStartEvent+bL;!this._target.rippleDisabled&&!e&&!i&&(this._isPointerDown=!0,this.fadeInRipple(n.clientX,n.clientY,this._target.rippleConfig))}_onTouchStart(n){if(!this._target.rippleDisabled&&!fs(n)){this._lastTouchStartEvent=Date.now(),this._isPointerDown=!0;let e=n.changedTouches;if(e)for(let i=0;i<e.length;i++)this.fadeInRipple(e[i].clientX,e[i].clientY,this._target.rippleConfig)}}_onPointerUp(){this._isPointerDown&&(this._isPointerDown=!1,this._getActiveRipples().forEach(n=>{let e=n.state===ni.VISIBLE||n.config.terminateOnPointerUp&&n.state===ni.FADING_IN;!n.config.persistent&&e&&n.fadeOut()}))}_getActiveRipples(){return Array.from(this._activeRipples.keys())}_removeTriggerEvents(){let n=this._triggerElement;n&&(ES.forEach(e=>t._eventManager.removeHandler(e,n,this)),this._pointerUpEventsRegistered&&(SS.forEach(e=>n.removeEventListener(e,this,DS)),this._pointerUpEventsRegistered=!1))}};function xL(t,n,e){let i=Math.max(Math.abs(t-e.left),Math.abs(t-e.right)),r=Math.max(Math.abs(n-e.top),Math.abs(n-e.bottom));return Math.sqrt(i*i+r*r)}var zl=new y("mat-ripple-global-options"),Xi=(()=>{class t{_elementRef=d(L);_animationsDisabled=ke();color;unbounded=!1;centered=!1;radius=0;animation;get disabled(){return this._disabled}set disabled(e){e&&this.fadeOutAllNonPersistent(),this._disabled=e,this._setupTriggerEventsIfEnabled()}_disabled=!1;get trigger(){return this._trigger||this._elementRef.nativeElement}set trigger(e){this._trigger=e,this._setupTriggerEventsIfEnabled()}_trigger;_rippleRenderer;_globalOptions;_isInitialized=!1;constructor(){let e=d(H),i=d(ye),r=d(zl,{optional:!0}),o=d(X);this._globalOptions=r||{},this._rippleRenderer=new _s(this,e,this._elementRef,i,o)}ngOnInit(){this._isInitialized=!0,this._setupTriggerEventsIfEnabled()}ngOnDestroy(){this._rippleRenderer._removeTriggerEvents()}fadeOutAll(){this._rippleRenderer.fadeOutAll()}fadeOutAllNonPersistent(){this._rippleRenderer.fadeOutAllNonPersistent()}get rippleConfig(){return{centered:this.centered,radius:this.radius,color:this.color,animation:b(b(b({},this._globalOptions.animation),this._animationsDisabled?{enterDuration:0,exitDuration:0}:{}),this.animation),terminateOnPointerUp:this._globalOptions.terminateOnPointerUp}}get rippleDisabled(){return this.disabled||!!this._globalOptions.disabled}_setupTriggerEventsIfEnabled(){!this.disabled&&this._isInitialized&&this._rippleRenderer.setupTriggerEvents(this.trigger)}launch(e,i=0,r){return typeof e=="number"?this._rippleRenderer.fadeInRipple(e,i,b(b({},this.rippleConfig),r)):this._rippleRenderer.fadeInRipple(0,0,b(b({},this.rippleConfig),e))}static \u0275fac=function(i){return new(i||t)};static \u0275dir=A({type:t,selectors:[["","mat-ripple",""],["","matRipple",""]],hostAttrs:[1,"mat-ripple"],hostVars:2,hostBindings:function(i,r){i&2&&z("mat-ripple-unbounded",r.unbounded)},inputs:{color:[0,"matRippleColor","color"],unbounded:[0,"matRippleUnbounded","unbounded"],centered:[0,"matRippleCentered","centered"],radius:[0,"matRippleRadius","radius"],animation:[0,"matRippleAnimation","animation"],disabled:[0,"matRippleDisabled","disabled"],trigger:[0,"matRippleTrigger","trigger"]},exportAs:["matRipple"]})}return t})();var CL={capture:!0},wL=["focus","mousedown","mouseenter","touchstart"],kb="mat-ripple-loader-uninitialized",Mb="mat-ripple-loader-class-name",IS="mat-ripple-loader-centered",If="mat-ripple-loader-disabled",kS=(()=>{class t{_document=d(K);_animationsDisabled=ke();_globalRippleOptions=d(zl,{optional:!0});_platform=d(ye);_ngZone=d(H);_injector=d(X);_eventCleanups;_hosts=new Map;constructor(){let e=d(Ot).createRenderer(null,null);this._eventCleanups=this._ngZone.runOutsideAngular(()=>wL.map(i=>e.listen(this._document,i,this._onInteraction,CL)))}ngOnDestroy(){let e=this._hosts.keys();for(let i of e)this.destroyRipple(i);this._eventCleanups.forEach(i=>i())}configureRipple(e,i){e.setAttribute(kb,this._globalRippleOptions?.namespace??""),(i.className||!e.hasAttribute(Mb))&&e.setAttribute(Mb,i.className||""),i.centered&&e.setAttribute(IS,""),i.disabled&&e.setAttribute(If,"")}setDisabled(e,i){let r=this._hosts.get(e);r?(r.target.rippleDisabled=i,!i&&!r.hasSetUpEvents&&(r.hasSetUpEvents=!0,r.renderer.setupTriggerEvents(e))):i?e.setAttribute(If,""):e.removeAttribute(If)}_onInteraction=e=>{let i=Kt(e);if(i instanceof HTMLElement){let r=i.closest(`[${kb}="${this._globalRippleOptions?.namespace??""}"]`);r&&this._createRipple(r)}};_createRipple(e){if(!this._document||this._hosts.has(e))return;e.querySelector(".mat-ripple")?.remove();let i=this._document.createElement("span");i.classList.add("mat-ripple",e.getAttribute(Mb)),e.append(i);let r=this._globalRippleOptions,o=this._animationsDisabled?0:r?.animation?.enterDuration??Ul.enterDuration,s=this._animationsDisabled?0:r?.animation?.exitDuration??Ul.exitDuration,a={rippleDisabled:this._animationsDisabled||r?.disabled||e.hasAttribute(If),rippleConfig:{centered:e.hasAttribute(IS),terminateOnPointerUp:r?.terminateOnPointerUp,animation:{enterDuration:o,exitDuration:s}}},c=new _s(a,this._ngZone,i,this._platform,this._injector),l=!a.rippleDisabled;l&&c.setupTriggerEvents(e),this._hosts.set(e,{target:a,renderer:c,hasSetUpEvents:l}),e.removeAttribute(kb)}destroyRipple(e){let i=this._hosts.get(e);i&&(i.renderer._removeTriggerEvents(),this._hosts.delete(e))}static \u0275fac=function(i){return new(i||t)};static \u0275prov=C({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();var An=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275cmp=E({type:t,selectors:[["structural-styles"]],decls:0,vars:0,template:function(i,r){},styles:[`.mat-focus-indicator {
  position: relative;
}
.mat-focus-indicator::before {
  top: 0;
  left: 0;
  right: 0;
  bottom: 0;
  position: absolute;
  box-sizing: border-box;
  pointer-events: none;
  display: var(--mat-focus-indicator-display, none);
  border-width: var(--mat-focus-indicator-border-width, 3px);
  border-style: var(--mat-focus-indicator-border-style, solid);
  border-color: var(--mat-focus-indicator-border-color, transparent);
  border-radius: var(--mat-focus-indicator-border-radius, 4px);
}
.mat-focus-indicator:focus-visible::before {
  content: "";
}

@media (forced-colors: active) {
  html {
    --mat-focus-indicator-display: block;
  }
}
`],encapsulation:2,changeDetection:0})}return t})();var DL=["mat-icon-button",""],EL=["*"],SL=new y("MAT_BUTTON_CONFIG");function MS(t){return t==null?void 0:lt(t)}var Tb=(()=>{class t{_elementRef=d(L);_ngZone=d(H);_animationsDisabled=ke();_config=d(SL,{optional:!0});_focusMonitor=d(Tn);_cleanupClick;_renderer=d(xe);_rippleLoader=d(kS);_isAnchor;_isFab=!1;color;get disableRipple(){return this._disableRipple}set disableRipple(e){this._disableRipple=e,this._updateRippleDisabled()}_disableRipple=!1;get disabled(){return this._disabled}set disabled(e){this._disabled=e,this._updateRippleDisabled()}_disabled=!1;ariaDisabled;disabledInteractive;tabIndex;set _tabindex(e){this.tabIndex=e}constructor(){d(qe).load(An);let e=this._elementRef.nativeElement;this._isAnchor=e.tagName==="A",this.disabledInteractive=this._config?.disabledInteractive??!1,this.color=this._config?.color??null,this._rippleLoader?.configureRipple(e,{className:"mat-mdc-button-ripple"})}ngAfterViewInit(){this._focusMonitor.monitor(this._elementRef,!0),this._isAnchor&&this._setupAsAnchor()}ngOnDestroy(){this._cleanupClick?.(),this._focusMonitor.stopMonitoring(this._elementRef),this._rippleLoader?.destroyRipple(this._elementRef.nativeElement)}focus(e="program",i){e?this._focusMonitor.focusVia(this._elementRef.nativeElement,e,i):this._elementRef.nativeElement.focus(i)}_getAriaDisabled(){return this.ariaDisabled!=null?this.ariaDisabled:this._isAnchor?this.disabled||null:this.disabled&&this.disabledInteractive?!0:null}_getDisabledAttribute(){return this.disabledInteractive||!this.disabled?null:!0}_updateRippleDisabled(){this._rippleLoader?.setDisabled(this._elementRef.nativeElement,this.disableRipple||this.disabled)}_getTabIndex(){return this._isAnchor?this.disabled&&!this.disabledInteractive?-1:this.tabIndex:this.tabIndex}_setupAsAnchor(){this._cleanupClick=this._ngZone.runOutsideAngular(()=>this._renderer.listen(this._elementRef.nativeElement,"click",e=>{this.disabled&&(e.preventDefault(),e.stopImmediatePropagation())}))}static \u0275fac=function(i){return new(i||t)};static \u0275dir=A({type:t,hostAttrs:[1,"mat-mdc-button-base"],hostVars:13,hostBindings:function(i,r){i&2&&(q("disabled",r._getDisabledAttribute())("aria-disabled",r._getAriaDisabled())("tabindex",r._getTabIndex()),Tt(r.color?"mat-"+r.color:""),z("mat-mdc-button-disabled",r.disabled)("mat-mdc-button-disabled-interactive",r.disabledInteractive)("mat-unthemed",!r.color)("_mat-animation-noopable",r._animationsDisabled))},inputs:{color:"color",disableRipple:[2,"disableRipple","disableRipple",B],disabled:[2,"disabled","disabled",B],ariaDisabled:[2,"aria-disabled","ariaDisabled",B],disabledInteractive:[2,"disabledInteractive","disabledInteractive",B],tabIndex:[2,"tabIndex","tabIndex",MS],_tabindex:[2,"tabindex","_tabindex",MS]}})}return t})(),$l=(()=>{class t extends Tb{constructor(){super(),this._rippleLoader.configureRipple(this._elementRef.nativeElement,{centered:!0})}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=E({type:t,selectors:[["button","mat-icon-button",""],["a","mat-icon-button",""],["button","matIconButton",""],["a","matIconButton",""]],hostAttrs:[1,"mdc-icon-button","mat-mdc-icon-button"],exportAs:["matButton","matAnchor"],features:[fe],attrs:DL,ngContentSelectors:EL,decls:4,vars:0,consts:[[1,"mat-mdc-button-persistent-ripple","mdc-icon-button__ripple"],[1,"mat-focus-indicator"],[1,"mat-mdc-button-touch-target"]],template:function(i,r){i&1&&(Ce(),Ht(0,"span",0),re(1),Ht(2,"span",1)(3,"span",2))},styles:[`.mat-mdc-icon-button {
  -webkit-user-select: none;
  user-select: none;
  display: inline-block;
  position: relative;
  box-sizing: border-box;
  border: none;
  outline: none;
  background-color: transparent;
  fill: currentColor;
  text-decoration: none;
  cursor: pointer;
  z-index: 0;
  overflow: visible;
  border-radius: var(--mat-icon-button-container-shape, var(--mat-sys-corner-full, 50%));
  flex-shrink: 0;
  text-align: center;
  width: var(--mat-icon-button-state-layer-size, 40px);
  height: var(--mat-icon-button-state-layer-size, 40px);
  padding: calc(calc(var(--mat-icon-button-state-layer-size, 40px) - var(--mat-icon-button-icon-size, 24px)) / 2);
  font-size: var(--mat-icon-button-icon-size, 24px);
  color: var(--mat-icon-button-icon-color, var(--mat-sys-on-surface-variant));
  -webkit-tap-highlight-color: transparent;
}
.mat-mdc-icon-button .mat-mdc-button-ripple,
.mat-mdc-icon-button .mat-mdc-button-persistent-ripple,
.mat-mdc-icon-button .mat-mdc-button-persistent-ripple::before {
  top: 0;
  left: 0;
  right: 0;
  bottom: 0;
  position: absolute;
  pointer-events: none;
  border-radius: inherit;
}
.mat-mdc-icon-button .mat-mdc-button-ripple {
  overflow: hidden;
}
.mat-mdc-icon-button .mat-mdc-button-persistent-ripple::before {
  content: "";
  opacity: 0;
}
.mat-mdc-icon-button .mdc-button__label,
.mat-mdc-icon-button .mat-icon {
  z-index: 1;
  position: relative;
}
.mat-mdc-icon-button .mat-focus-indicator {
  top: 0;
  left: 0;
  right: 0;
  bottom: 0;
  position: absolute;
  border-radius: inherit;
}
.mat-mdc-icon-button:focus-visible > .mat-focus-indicator::before {
  content: "";
  border-radius: inherit;
}
.mat-mdc-icon-button .mat-ripple-element {
  background-color: var(--mat-icon-button-ripple-color, color-mix(in srgb, var(--mat-sys-on-surface-variant) calc(var(--mat-sys-pressed-state-layer-opacity) * 100%), transparent));
}
.mat-mdc-icon-button .mat-mdc-button-persistent-ripple::before {
  background-color: var(--mat-icon-button-state-layer-color, var(--mat-sys-on-surface-variant));
}
.mat-mdc-icon-button.mat-mdc-button-disabled .mat-mdc-button-persistent-ripple::before {
  background-color: var(--mat-icon-button-disabled-state-layer-color, var(--mat-sys-on-surface-variant));
}
.mat-mdc-icon-button:hover > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--mat-icon-button-hover-state-layer-opacity, var(--mat-sys-hover-state-layer-opacity));
}
.mat-mdc-icon-button.cdk-program-focused > .mat-mdc-button-persistent-ripple::before, .mat-mdc-icon-button.cdk-keyboard-focused > .mat-mdc-button-persistent-ripple::before, .mat-mdc-icon-button.mat-mdc-button-disabled-interactive:focus > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--mat-icon-button-focus-state-layer-opacity, var(--mat-sys-focus-state-layer-opacity));
}
.mat-mdc-icon-button:active > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--mat-icon-button-pressed-state-layer-opacity, var(--mat-sys-pressed-state-layer-opacity));
}
.mat-mdc-icon-button .mat-mdc-button-touch-target {
  position: absolute;
  top: 50%;
  height: var(--mat-icon-button-touch-target-size, 48px);
  display: var(--mat-icon-button-touch-target-display, block);
  left: 50%;
  width: var(--mat-icon-button-touch-target-size, 48px);
  transform: translate(-50%, -50%);
}
.mat-mdc-icon-button._mat-animation-noopable {
  transition: none !important;
  animation: none !important;
}
.mat-mdc-icon-button[disabled], .mat-mdc-icon-button.mat-mdc-button-disabled {
  cursor: default;
  pointer-events: none;
  color: var(--mat-icon-button-disabled-icon-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
}
.mat-mdc-icon-button.mat-mdc-button-disabled-interactive {
  pointer-events: auto;
}
.mat-mdc-icon-button img,
.mat-mdc-icon-button svg {
  width: var(--mat-icon-button-icon-size, 24px);
  height: var(--mat-icon-button-icon-size, 24px);
  vertical-align: baseline;
}
.mat-mdc-icon-button .mat-mdc-button-persistent-ripple {
  border-radius: var(--mat-icon-button-container-shape, var(--mat-sys-corner-full, 50%));
}
.mat-mdc-icon-button[hidden] {
  display: none;
}
.mat-mdc-icon-button.mat-unthemed:not(.mdc-ripple-upgraded):focus::before, .mat-mdc-icon-button.mat-primary:not(.mdc-ripple-upgraded):focus::before, .mat-mdc-icon-button.mat-accent:not(.mdc-ripple-upgraded):focus::before, .mat-mdc-icon-button.mat-warn:not(.mdc-ripple-upgraded):focus::before {
  background: transparent;
  opacity: 1;
}
`,`@media (forced-colors: active) {
  .mat-mdc-button:not(.mdc-button--outlined),
  .mat-mdc-unelevated-button:not(.mdc-button--outlined),
  .mat-mdc-raised-button:not(.mdc-button--outlined),
  .mat-mdc-outlined-button:not(.mdc-button--outlined),
  .mat-mdc-button-base.mat-tonal-button,
  .mat-mdc-icon-button.mat-mdc-icon-button,
  .mat-mdc-outlined-button .mdc-button__ripple {
    outline: solid 1px;
  }
}
`],encapsulation:2,changeDetection:0})}return t})();var kf=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=ae({type:t});static \u0275inj=oe({imports:[Yt]})}return t})();var IL=["matButton",""],kL=[[["",8,"material-icons",3,"iconPositionEnd",""],["mat-icon",3,"iconPositionEnd",""],["","matButtonIcon","",3,"iconPositionEnd",""]],"*",[["","iconPositionEnd","",8,"material-icons"],["mat-icon","iconPositionEnd",""],["","matButtonIcon","","iconPositionEnd",""]]],ML=[".material-icons:not([iconPositionEnd]), mat-icon:not([iconPositionEnd]), [matButtonIcon]:not([iconPositionEnd])","*",".material-icons[iconPositionEnd], mat-icon[iconPositionEnd], [matButtonIcon][iconPositionEnd]"];var TS=new Map([["text",["mat-mdc-button"]],["filled",["mdc-button--unelevated","mat-mdc-unelevated-button"]],["elevated",["mdc-button--raised","mat-mdc-raised-button"]],["outlined",["mdc-button--outlined","mat-mdc-outlined-button"]],["tonal",["mat-tonal-button"]]]),Pe=(()=>{class t extends Tb{get appearance(){return this._appearance}set appearance(e){this.setAppearance(e||this._config?.defaultAppearance||"text")}_appearance=null;constructor(){super();let e=TL(this._elementRef.nativeElement);e&&this.setAppearance(e)}setAppearance(e){if(e===this._appearance)return;let i=this._elementRef.nativeElement.classList,r=this._appearance?TS.get(this._appearance):null,o=TS.get(e);r&&i.remove(...r),i.add(...o),this._appearance=e}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=E({type:t,selectors:[["button","matButton",""],["a","matButton",""],["button","mat-button",""],["button","mat-raised-button",""],["button","mat-flat-button",""],["button","mat-stroked-button",""],["a","mat-button",""],["a","mat-raised-button",""],["a","mat-flat-button",""],["a","mat-stroked-button",""]],hostAttrs:[1,"mdc-button"],inputs:{appearance:[0,"matButton","appearance"]},exportAs:["matButton","matAnchor"],features:[fe],attrs:IL,ngContentSelectors:ML,decls:7,vars:4,consts:[[1,"mat-mdc-button-persistent-ripple"],[1,"mdc-button__label"],[1,"mat-focus-indicator"],[1,"mat-mdc-button-touch-target"]],template:function(i,r){i&1&&(Ce(kL),Ht(0,"span",0),re(1),Fe(2,"span",1),re(3,1),Ge(),re(4,2),Ht(5,"span",2)(6,"span",3)),i&2&&z("mdc-button__ripple",!r._isFab)("mdc-fab__ripple",r._isFab)},styles:[`.mat-mdc-button-base {
  text-decoration: none;
}
.mat-mdc-button-base .mat-icon {
  min-height: fit-content;
  flex-shrink: 0;
}
@media (hover: none) {
  .mat-mdc-button-base:hover > span.mat-mdc-button-persistent-ripple::before {
    opacity: 0;
  }
}

.mdc-button {
  -webkit-user-select: none;
  user-select: none;
  position: relative;
  display: inline-flex;
  align-items: center;
  justify-content: center;
  box-sizing: border-box;
  min-width: 64px;
  border: none;
  outline: none;
  line-height: inherit;
  -webkit-appearance: none;
  overflow: visible;
  vertical-align: middle;
  background: transparent;
  padding: 0 8px;
}
.mdc-button::-moz-focus-inner {
  padding: 0;
  border: 0;
}
.mdc-button:active {
  outline: none;
}
.mdc-button:hover {
  cursor: pointer;
}
.mdc-button:disabled {
  cursor: default;
  pointer-events: none;
}
.mdc-button[hidden] {
  display: none;
}
.mdc-button .mdc-button__label {
  position: relative;
}

.mat-mdc-button {
  padding: 0 var(--mat-button-text-horizontal-padding, 12px);
  height: var(--mat-button-text-container-height, 40px);
  font-family: var(--mat-button-text-label-text-font, var(--mat-sys-label-large-font));
  font-size: var(--mat-button-text-label-text-size, var(--mat-sys-label-large-size));
  letter-spacing: var(--mat-button-text-label-text-tracking, var(--mat-sys-label-large-tracking));
  text-transform: var(--mat-button-text-label-text-transform);
  font-weight: var(--mat-button-text-label-text-weight, var(--mat-sys-label-large-weight));
}
.mat-mdc-button, .mat-mdc-button .mdc-button__ripple {
  border-radius: var(--mat-button-text-container-shape, var(--mat-sys-corner-full));
}
.mat-mdc-button:not(:disabled) {
  color: var(--mat-button-text-label-text-color, var(--mat-sys-primary));
}
.mat-mdc-button[disabled], .mat-mdc-button.mat-mdc-button-disabled {
  cursor: default;
  pointer-events: none;
  color: var(--mat-button-text-disabled-label-text-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
}
.mat-mdc-button.mat-mdc-button-disabled-interactive {
  pointer-events: auto;
}
.mat-mdc-button:has(.material-icons, mat-icon, [matButtonIcon]) {
  padding: 0 var(--mat-button-text-with-icon-horizontal-padding, 16px);
}
.mat-mdc-button > .mat-icon {
  margin-right: var(--mat-button-text-icon-spacing, 8px);
  margin-left: var(--mat-button-text-icon-offset, -4px);
}
[dir=rtl] .mat-mdc-button > .mat-icon {
  margin-right: var(--mat-button-text-icon-offset, -4px);
  margin-left: var(--mat-button-text-icon-spacing, 8px);
}
.mat-mdc-button .mdc-button__label + .mat-icon {
  margin-right: var(--mat-button-text-icon-offset, -4px);
  margin-left: var(--mat-button-text-icon-spacing, 8px);
}
[dir=rtl] .mat-mdc-button .mdc-button__label + .mat-icon {
  margin-right: var(--mat-button-text-icon-spacing, 8px);
  margin-left: var(--mat-button-text-icon-offset, -4px);
}
.mat-mdc-button .mat-ripple-element {
  background-color: var(--mat-button-text-ripple-color, color-mix(in srgb, var(--mat-sys-primary) calc(var(--mat-sys-pressed-state-layer-opacity) * 100%), transparent));
}
.mat-mdc-button .mat-mdc-button-persistent-ripple::before {
  background-color: var(--mat-button-text-state-layer-color, var(--mat-sys-primary));
}
.mat-mdc-button.mat-mdc-button-disabled .mat-mdc-button-persistent-ripple::before {
  background-color: var(--mat-button-text-disabled-state-layer-color, var(--mat-sys-on-surface-variant));
}
.mat-mdc-button:hover > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--mat-button-text-hover-state-layer-opacity, var(--mat-sys-hover-state-layer-opacity));
}
.mat-mdc-button.cdk-program-focused > .mat-mdc-button-persistent-ripple::before, .mat-mdc-button.cdk-keyboard-focused > .mat-mdc-button-persistent-ripple::before, .mat-mdc-button.mat-mdc-button-disabled-interactive:focus > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--mat-button-text-focus-state-layer-opacity, var(--mat-sys-focus-state-layer-opacity));
}
.mat-mdc-button:active > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--mat-button-text-pressed-state-layer-opacity, var(--mat-sys-pressed-state-layer-opacity));
}
.mat-mdc-button .mat-mdc-button-touch-target {
  position: absolute;
  top: 50%;
  height: var(--mat-button-text-touch-target-size, 48px);
  display: var(--mat-button-text-touch-target-display, block);
  left: 0;
  right: 0;
  transform: translateY(-50%);
}

.mat-mdc-unelevated-button {
  transition: box-shadow 280ms cubic-bezier(0.4, 0, 0.2, 1);
  height: var(--mat-button-filled-container-height, 40px);
  font-family: var(--mat-button-filled-label-text-font, var(--mat-sys-label-large-font));
  font-size: var(--mat-button-filled-label-text-size, var(--mat-sys-label-large-size));
  letter-spacing: var(--mat-button-filled-label-text-tracking, var(--mat-sys-label-large-tracking));
  text-transform: var(--mat-button-filled-label-text-transform);
  font-weight: var(--mat-button-filled-label-text-weight, var(--mat-sys-label-large-weight));
  padding: 0 var(--mat-button-filled-horizontal-padding, 24px);
}
.mat-mdc-unelevated-button > .mat-icon {
  margin-right: var(--mat-button-filled-icon-spacing, 8px);
  margin-left: var(--mat-button-filled-icon-offset, -8px);
}
[dir=rtl] .mat-mdc-unelevated-button > .mat-icon {
  margin-right: var(--mat-button-filled-icon-offset, -8px);
  margin-left: var(--mat-button-filled-icon-spacing, 8px);
}
.mat-mdc-unelevated-button .mdc-button__label + .mat-icon {
  margin-right: var(--mat-button-filled-icon-offset, -8px);
  margin-left: var(--mat-button-filled-icon-spacing, 8px);
}
[dir=rtl] .mat-mdc-unelevated-button .mdc-button__label + .mat-icon {
  margin-right: var(--mat-button-filled-icon-spacing, 8px);
  margin-left: var(--mat-button-filled-icon-offset, -8px);
}
.mat-mdc-unelevated-button .mat-ripple-element {
  background-color: var(--mat-button-filled-ripple-color, color-mix(in srgb, var(--mat-sys-on-primary) calc(var(--mat-sys-pressed-state-layer-opacity) * 100%), transparent));
}
.mat-mdc-unelevated-button .mat-mdc-button-persistent-ripple::before {
  background-color: var(--mat-button-filled-state-layer-color, var(--mat-sys-on-primary));
}
.mat-mdc-unelevated-button.mat-mdc-button-disabled .mat-mdc-button-persistent-ripple::before {
  background-color: var(--mat-button-filled-disabled-state-layer-color, var(--mat-sys-on-surface-variant));
}
.mat-mdc-unelevated-button:hover > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--mat-button-filled-hover-state-layer-opacity, var(--mat-sys-hover-state-layer-opacity));
}
.mat-mdc-unelevated-button.cdk-program-focused > .mat-mdc-button-persistent-ripple::before, .mat-mdc-unelevated-button.cdk-keyboard-focused > .mat-mdc-button-persistent-ripple::before, .mat-mdc-unelevated-button.mat-mdc-button-disabled-interactive:focus > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--mat-button-filled-focus-state-layer-opacity, var(--mat-sys-focus-state-layer-opacity));
}
.mat-mdc-unelevated-button:active > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--mat-button-filled-pressed-state-layer-opacity, var(--mat-sys-pressed-state-layer-opacity));
}
.mat-mdc-unelevated-button .mat-mdc-button-touch-target {
  position: absolute;
  top: 50%;
  height: var(--mat-button-filled-touch-target-size, 48px);
  display: var(--mat-button-filled-touch-target-display, block);
  left: 0;
  right: 0;
  transform: translateY(-50%);
}
.mat-mdc-unelevated-button:not(:disabled) {
  color: var(--mat-button-filled-label-text-color, var(--mat-sys-on-primary));
  background-color: var(--mat-button-filled-container-color, var(--mat-sys-primary));
}
.mat-mdc-unelevated-button, .mat-mdc-unelevated-button .mdc-button__ripple {
  border-radius: var(--mat-button-filled-container-shape, var(--mat-sys-corner-full));
}
.mat-mdc-unelevated-button[disabled], .mat-mdc-unelevated-button.mat-mdc-button-disabled {
  cursor: default;
  pointer-events: none;
  color: var(--mat-button-filled-disabled-label-text-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
  background-color: var(--mat-button-filled-disabled-container-color, color-mix(in srgb, var(--mat-sys-on-surface) 12%, transparent));
}
.mat-mdc-unelevated-button.mat-mdc-button-disabled-interactive {
  pointer-events: auto;
}

.mat-mdc-raised-button {
  transition: box-shadow 280ms cubic-bezier(0.4, 0, 0.2, 1);
  box-shadow: var(--mat-button-protected-container-elevation-shadow, var(--mat-sys-level1));
  height: var(--mat-button-protected-container-height, 40px);
  font-family: var(--mat-button-protected-label-text-font, var(--mat-sys-label-large-font));
  font-size: var(--mat-button-protected-label-text-size, var(--mat-sys-label-large-size));
  letter-spacing: var(--mat-button-protected-label-text-tracking, var(--mat-sys-label-large-tracking));
  text-transform: var(--mat-button-protected-label-text-transform);
  font-weight: var(--mat-button-protected-label-text-weight, var(--mat-sys-label-large-weight));
  padding: 0 var(--mat-button-protected-horizontal-padding, 24px);
}
.mat-mdc-raised-button > .mat-icon {
  margin-right: var(--mat-button-protected-icon-spacing, 8px);
  margin-left: var(--mat-button-protected-icon-offset, -8px);
}
[dir=rtl] .mat-mdc-raised-button > .mat-icon {
  margin-right: var(--mat-button-protected-icon-offset, -8px);
  margin-left: var(--mat-button-protected-icon-spacing, 8px);
}
.mat-mdc-raised-button .mdc-button__label + .mat-icon {
  margin-right: var(--mat-button-protected-icon-offset, -8px);
  margin-left: var(--mat-button-protected-icon-spacing, 8px);
}
[dir=rtl] .mat-mdc-raised-button .mdc-button__label + .mat-icon {
  margin-right: var(--mat-button-protected-icon-spacing, 8px);
  margin-left: var(--mat-button-protected-icon-offset, -8px);
}
.mat-mdc-raised-button .mat-ripple-element {
  background-color: var(--mat-button-protected-ripple-color, color-mix(in srgb, var(--mat-sys-primary) calc(var(--mat-sys-pressed-state-layer-opacity) * 100%), transparent));
}
.mat-mdc-raised-button .mat-mdc-button-persistent-ripple::before {
  background-color: var(--mat-button-protected-state-layer-color, var(--mat-sys-primary));
}
.mat-mdc-raised-button.mat-mdc-button-disabled .mat-mdc-button-persistent-ripple::before {
  background-color: var(--mat-button-protected-disabled-state-layer-color, var(--mat-sys-on-surface-variant));
}
.mat-mdc-raised-button:hover > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--mat-button-protected-hover-state-layer-opacity, var(--mat-sys-hover-state-layer-opacity));
}
.mat-mdc-raised-button.cdk-program-focused > .mat-mdc-button-persistent-ripple::before, .mat-mdc-raised-button.cdk-keyboard-focused > .mat-mdc-button-persistent-ripple::before, .mat-mdc-raised-button.mat-mdc-button-disabled-interactive:focus > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--mat-button-protected-focus-state-layer-opacity, var(--mat-sys-focus-state-layer-opacity));
}
.mat-mdc-raised-button:active > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--mat-button-protected-pressed-state-layer-opacity, var(--mat-sys-pressed-state-layer-opacity));
}
.mat-mdc-raised-button .mat-mdc-button-touch-target {
  position: absolute;
  top: 50%;
  height: var(--mat-button-protected-touch-target-size, 48px);
  display: var(--mat-button-protected-touch-target-display, block);
  left: 0;
  right: 0;
  transform: translateY(-50%);
}
.mat-mdc-raised-button:not(:disabled) {
  color: var(--mat-button-protected-label-text-color, var(--mat-sys-primary));
  background-color: var(--mat-button-protected-container-color, var(--mat-sys-surface));
}
.mat-mdc-raised-button, .mat-mdc-raised-button .mdc-button__ripple {
  border-radius: var(--mat-button-protected-container-shape, var(--mat-sys-corner-full));
}
@media (hover: hover) {
  .mat-mdc-raised-button:hover {
    box-shadow: var(--mat-button-protected-hover-container-elevation-shadow, var(--mat-sys-level2));
  }
}
.mat-mdc-raised-button:focus {
  box-shadow: var(--mat-button-protected-focus-container-elevation-shadow, var(--mat-sys-level1));
}
.mat-mdc-raised-button:active, .mat-mdc-raised-button:focus:active {
  box-shadow: var(--mat-button-protected-pressed-container-elevation-shadow, var(--mat-sys-level1));
}
.mat-mdc-raised-button[disabled], .mat-mdc-raised-button.mat-mdc-button-disabled {
  cursor: default;
  pointer-events: none;
  color: var(--mat-button-protected-disabled-label-text-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
  background-color: var(--mat-button-protected-disabled-container-color, color-mix(in srgb, var(--mat-sys-on-surface) 12%, transparent));
}
.mat-mdc-raised-button[disabled].mat-mdc-button-disabled, .mat-mdc-raised-button.mat-mdc-button-disabled.mat-mdc-button-disabled {
  box-shadow: var(--mat-button-protected-disabled-container-elevation-shadow, var(--mat-sys-level0));
}
.mat-mdc-raised-button.mat-mdc-button-disabled-interactive {
  pointer-events: auto;
}

.mat-mdc-outlined-button {
  border-style: solid;
  transition: border 280ms cubic-bezier(0.4, 0, 0.2, 1);
  height: var(--mat-button-outlined-container-height, 40px);
  font-family: var(--mat-button-outlined-label-text-font, var(--mat-sys-label-large-font));
  font-size: var(--mat-button-outlined-label-text-size, var(--mat-sys-label-large-size));
  letter-spacing: var(--mat-button-outlined-label-text-tracking, var(--mat-sys-label-large-tracking));
  text-transform: var(--mat-button-outlined-label-text-transform);
  font-weight: var(--mat-button-outlined-label-text-weight, var(--mat-sys-label-large-weight));
  border-radius: var(--mat-button-outlined-container-shape, var(--mat-sys-corner-full));
  border-width: var(--mat-button-outlined-outline-width, 1px);
  padding: 0 var(--mat-button-outlined-horizontal-padding, 24px);
}
.mat-mdc-outlined-button > .mat-icon {
  margin-right: var(--mat-button-outlined-icon-spacing, 8px);
  margin-left: var(--mat-button-outlined-icon-offset, -8px);
}
[dir=rtl] .mat-mdc-outlined-button > .mat-icon {
  margin-right: var(--mat-button-outlined-icon-offset, -8px);
  margin-left: var(--mat-button-outlined-icon-spacing, 8px);
}
.mat-mdc-outlined-button .mdc-button__label + .mat-icon {
  margin-right: var(--mat-button-outlined-icon-offset, -8px);
  margin-left: var(--mat-button-outlined-icon-spacing, 8px);
}
[dir=rtl] .mat-mdc-outlined-button .mdc-button__label + .mat-icon {
  margin-right: var(--mat-button-outlined-icon-spacing, 8px);
  margin-left: var(--mat-button-outlined-icon-offset, -8px);
}
.mat-mdc-outlined-button .mat-ripple-element {
  background-color: var(--mat-button-outlined-ripple-color, color-mix(in srgb, var(--mat-sys-primary) calc(var(--mat-sys-pressed-state-layer-opacity) * 100%), transparent));
}
.mat-mdc-outlined-button .mat-mdc-button-persistent-ripple::before {
  background-color: var(--mat-button-outlined-state-layer-color, var(--mat-sys-primary));
}
.mat-mdc-outlined-button.mat-mdc-button-disabled .mat-mdc-button-persistent-ripple::before {
  background-color: var(--mat-button-outlined-disabled-state-layer-color, var(--mat-sys-on-surface-variant));
}
.mat-mdc-outlined-button:hover > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--mat-button-outlined-hover-state-layer-opacity, var(--mat-sys-hover-state-layer-opacity));
}
.mat-mdc-outlined-button.cdk-program-focused > .mat-mdc-button-persistent-ripple::before, .mat-mdc-outlined-button.cdk-keyboard-focused > .mat-mdc-button-persistent-ripple::before, .mat-mdc-outlined-button.mat-mdc-button-disabled-interactive:focus > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--mat-button-outlined-focus-state-layer-opacity, var(--mat-sys-focus-state-layer-opacity));
}
.mat-mdc-outlined-button:active > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--mat-button-outlined-pressed-state-layer-opacity, var(--mat-sys-pressed-state-layer-opacity));
}
.mat-mdc-outlined-button .mat-mdc-button-touch-target {
  position: absolute;
  top: 50%;
  height: var(--mat-button-outlined-touch-target-size, 48px);
  display: var(--mat-button-outlined-touch-target-display, block);
  left: 0;
  right: 0;
  transform: translateY(-50%);
}
.mat-mdc-outlined-button:not(:disabled) {
  color: var(--mat-button-outlined-label-text-color, var(--mat-sys-primary));
  border-color: var(--mat-button-outlined-outline-color, var(--mat-sys-outline));
}
.mat-mdc-outlined-button[disabled], .mat-mdc-outlined-button.mat-mdc-button-disabled {
  cursor: default;
  pointer-events: none;
  color: var(--mat-button-outlined-disabled-label-text-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
  border-color: var(--mat-button-outlined-disabled-outline-color, color-mix(in srgb, var(--mat-sys-on-surface) 12%, transparent));
}
.mat-mdc-outlined-button.mat-mdc-button-disabled-interactive {
  pointer-events: auto;
}

.mat-tonal-button {
  transition: box-shadow 280ms cubic-bezier(0.4, 0, 0.2, 1);
  height: var(--mat-button-tonal-container-height, 40px);
  font-family: var(--mat-button-tonal-label-text-font, var(--mat-sys-label-large-font));
  font-size: var(--mat-button-tonal-label-text-size, var(--mat-sys-label-large-size));
  letter-spacing: var(--mat-button-tonal-label-text-tracking, var(--mat-sys-label-large-tracking));
  text-transform: var(--mat-button-tonal-label-text-transform);
  font-weight: var(--mat-button-tonal-label-text-weight, var(--mat-sys-label-large-weight));
  padding: 0 var(--mat-button-tonal-horizontal-padding, 24px);
}
.mat-tonal-button:not(:disabled) {
  color: var(--mat-button-tonal-label-text-color, var(--mat-sys-on-secondary-container));
  background-color: var(--mat-button-tonal-container-color, var(--mat-sys-secondary-container));
}
.mat-tonal-button, .mat-tonal-button .mdc-button__ripple {
  border-radius: var(--mat-button-tonal-container-shape, var(--mat-sys-corner-full));
}
.mat-tonal-button[disabled], .mat-tonal-button.mat-mdc-button-disabled {
  cursor: default;
  pointer-events: none;
  color: var(--mat-button-tonal-disabled-label-text-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
  background-color: var(--mat-button-tonal-disabled-container-color, color-mix(in srgb, var(--mat-sys-on-surface) 12%, transparent));
}
.mat-tonal-button.mat-mdc-button-disabled-interactive {
  pointer-events: auto;
}
.mat-tonal-button > .mat-icon {
  margin-right: var(--mat-button-tonal-icon-spacing, 8px);
  margin-left: var(--mat-button-tonal-icon-offset, -8px);
}
[dir=rtl] .mat-tonal-button > .mat-icon {
  margin-right: var(--mat-button-tonal-icon-offset, -8px);
  margin-left: var(--mat-button-tonal-icon-spacing, 8px);
}
.mat-tonal-button .mdc-button__label + .mat-icon {
  margin-right: var(--mat-button-tonal-icon-offset, -8px);
  margin-left: var(--mat-button-tonal-icon-spacing, 8px);
}
[dir=rtl] .mat-tonal-button .mdc-button__label + .mat-icon {
  margin-right: var(--mat-button-tonal-icon-spacing, 8px);
  margin-left: var(--mat-button-tonal-icon-offset, -8px);
}
.mat-tonal-button .mat-ripple-element {
  background-color: var(--mat-button-tonal-ripple-color, color-mix(in srgb, var(--mat-sys-on-secondary-container) calc(var(--mat-sys-pressed-state-layer-opacity) * 100%), transparent));
}
.mat-tonal-button .mat-mdc-button-persistent-ripple::before {
  background-color: var(--mat-button-tonal-state-layer-color, var(--mat-sys-on-secondary-container));
}
.mat-tonal-button.mat-mdc-button-disabled .mat-mdc-button-persistent-ripple::before {
  background-color: var(--mat-button-tonal-disabled-state-layer-color, var(--mat-sys-on-surface-variant));
}
.mat-tonal-button:hover > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--mat-button-tonal-hover-state-layer-opacity, var(--mat-sys-hover-state-layer-opacity));
}
.mat-tonal-button.cdk-program-focused > .mat-mdc-button-persistent-ripple::before, .mat-tonal-button.cdk-keyboard-focused > .mat-mdc-button-persistent-ripple::before, .mat-tonal-button.mat-mdc-button-disabled-interactive:focus > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--mat-button-tonal-focus-state-layer-opacity, var(--mat-sys-focus-state-layer-opacity));
}
.mat-tonal-button:active > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--mat-button-tonal-pressed-state-layer-opacity, var(--mat-sys-pressed-state-layer-opacity));
}
.mat-tonal-button .mat-mdc-button-touch-target {
  position: absolute;
  top: 50%;
  height: var(--mat-button-tonal-touch-target-size, 48px);
  display: var(--mat-button-tonal-touch-target-display, block);
  left: 0;
  right: 0;
  transform: translateY(-50%);
}

.mat-mdc-button,
.mat-mdc-unelevated-button,
.mat-mdc-raised-button,
.mat-mdc-outlined-button,
.mat-tonal-button {
  -webkit-tap-highlight-color: transparent;
}
.mat-mdc-button .mat-mdc-button-ripple,
.mat-mdc-button .mat-mdc-button-persistent-ripple,
.mat-mdc-button .mat-mdc-button-persistent-ripple::before,
.mat-mdc-unelevated-button .mat-mdc-button-ripple,
.mat-mdc-unelevated-button .mat-mdc-button-persistent-ripple,
.mat-mdc-unelevated-button .mat-mdc-button-persistent-ripple::before,
.mat-mdc-raised-button .mat-mdc-button-ripple,
.mat-mdc-raised-button .mat-mdc-button-persistent-ripple,
.mat-mdc-raised-button .mat-mdc-button-persistent-ripple::before,
.mat-mdc-outlined-button .mat-mdc-button-ripple,
.mat-mdc-outlined-button .mat-mdc-button-persistent-ripple,
.mat-mdc-outlined-button .mat-mdc-button-persistent-ripple::before,
.mat-tonal-button .mat-mdc-button-ripple,
.mat-tonal-button .mat-mdc-button-persistent-ripple,
.mat-tonal-button .mat-mdc-button-persistent-ripple::before {
  top: 0;
  left: 0;
  right: 0;
  bottom: 0;
  position: absolute;
  pointer-events: none;
  border-radius: inherit;
}
.mat-mdc-button .mat-mdc-button-ripple,
.mat-mdc-unelevated-button .mat-mdc-button-ripple,
.mat-mdc-raised-button .mat-mdc-button-ripple,
.mat-mdc-outlined-button .mat-mdc-button-ripple,
.mat-tonal-button .mat-mdc-button-ripple {
  overflow: hidden;
}
.mat-mdc-button .mat-mdc-button-persistent-ripple::before,
.mat-mdc-unelevated-button .mat-mdc-button-persistent-ripple::before,
.mat-mdc-raised-button .mat-mdc-button-persistent-ripple::before,
.mat-mdc-outlined-button .mat-mdc-button-persistent-ripple::before,
.mat-tonal-button .mat-mdc-button-persistent-ripple::before {
  content: "";
  opacity: 0;
}
.mat-mdc-button .mdc-button__label,
.mat-mdc-button .mat-icon,
.mat-mdc-unelevated-button .mdc-button__label,
.mat-mdc-unelevated-button .mat-icon,
.mat-mdc-raised-button .mdc-button__label,
.mat-mdc-raised-button .mat-icon,
.mat-mdc-outlined-button .mdc-button__label,
.mat-mdc-outlined-button .mat-icon,
.mat-tonal-button .mdc-button__label,
.mat-tonal-button .mat-icon {
  z-index: 1;
  position: relative;
}
.mat-mdc-button .mat-focus-indicator,
.mat-mdc-unelevated-button .mat-focus-indicator,
.mat-mdc-raised-button .mat-focus-indicator,
.mat-mdc-outlined-button .mat-focus-indicator,
.mat-tonal-button .mat-focus-indicator {
  top: 0;
  left: 0;
  right: 0;
  bottom: 0;
  position: absolute;
  border-radius: inherit;
}
.mat-mdc-button:focus-visible > .mat-focus-indicator::before,
.mat-mdc-unelevated-button:focus-visible > .mat-focus-indicator::before,
.mat-mdc-raised-button:focus-visible > .mat-focus-indicator::before,
.mat-mdc-outlined-button:focus-visible > .mat-focus-indicator::before,
.mat-tonal-button:focus-visible > .mat-focus-indicator::before {
  content: "";
  border-radius: inherit;
}
.mat-mdc-button._mat-animation-noopable,
.mat-mdc-unelevated-button._mat-animation-noopable,
.mat-mdc-raised-button._mat-animation-noopable,
.mat-mdc-outlined-button._mat-animation-noopable,
.mat-tonal-button._mat-animation-noopable {
  transition: none !important;
  animation: none !important;
}
.mat-mdc-button > .mat-icon,
.mat-mdc-unelevated-button > .mat-icon,
.mat-mdc-raised-button > .mat-icon,
.mat-mdc-outlined-button > .mat-icon,
.mat-tonal-button > .mat-icon {
  display: inline-block;
  position: relative;
  vertical-align: top;
  font-size: 1.125rem;
  height: 1.125rem;
  width: 1.125rem;
}

.mat-mdc-outlined-button .mat-mdc-button-ripple,
.mat-mdc-outlined-button .mdc-button__ripple {
  top: -1px;
  left: -1px;
  bottom: -1px;
  right: -1px;
}

.mat-mdc-unelevated-button .mat-focus-indicator::before,
.mat-tonal-button .mat-focus-indicator::before,
.mat-mdc-raised-button .mat-focus-indicator::before {
  margin: calc(calc(var(--mat-focus-indicator-border-width, 3px) + 2px) * -1);
}

.mat-mdc-outlined-button .mat-focus-indicator::before {
  margin: calc(calc(var(--mat-focus-indicator-border-width, 3px) + 3px) * -1);
}
`,`@media (forced-colors: active) {
  .mat-mdc-button:not(.mdc-button--outlined),
  .mat-mdc-unelevated-button:not(.mdc-button--outlined),
  .mat-mdc-raised-button:not(.mdc-button--outlined),
  .mat-mdc-outlined-button:not(.mdc-button--outlined),
  .mat-mdc-button-base.mat-tonal-button,
  .mat-mdc-icon-button.mat-mdc-icon-button,
  .mat-mdc-outlined-button .mdc-button__ripple {
    outline: solid 1px;
  }
}
`],encapsulation:2,changeDetection:0})}return t})();function TL(t){return t.hasAttribute("mat-raised-button")?"elevated":t.hasAttribute("mat-stroked-button")?"outlined":t.hasAttribute("mat-flat-button")?"filled":t.hasAttribute("mat-button")?"text":null}function AS(t){return Error(`Unable to find icon with the name "${t}"`)}function AL(){return Error("Could not find HttpClient for use with Angular Material icons. Please add provideHttpClient() to your providers.")}function RS(t){return Error(`The URL provided to MatIconRegistry was not trusted as a resource URL via Angular's DomSanitizer. Attempted URL was "${t}".`)}function OS(t){return Error(`The literal provided to MatIconRegistry was not trusted as safe HTML by Angular's DomSanitizer. Attempted literal was "${t}".`)}var Cr=class{url;svgText;options;svgElement=null;constructor(n,e,i){this.url=n,this.svgText=e,this.options=i}},PS=(()=>{class t{_httpClient;_sanitizer;_errorHandler;_document;_svgIconConfigs=new Map;_iconSetConfigs=new Map;_cachedIconsByUrl=new Map;_inProgressUrlFetches=new Map;_fontCssClassesByAlias=new Map;_resolvers=[];_defaultFontSetClass=["material-icons","mat-ligature-font"];constructor(e,i,r,o){this._httpClient=e,this._sanitizer=i,this._errorHandler=o,this._document=r}addSvgIcon(e,i,r){return this.addSvgIconInNamespace("",e,i,r)}addSvgIconLiteral(e,i,r){return this.addSvgIconLiteralInNamespace("",e,i,r)}addSvgIconInNamespace(e,i,r,o){return this._addSvgIconConfig(e,i,new Cr(r,null,o))}addSvgIconResolver(e){return this._resolvers.push(e),this}addSvgIconLiteralInNamespace(e,i,r,o){let s=this._sanitizer.sanitize(Bt.HTML,r);if(!s)throw OS(r);let a=hs(s);return this._addSvgIconConfig(e,i,new Cr("",a,o))}addSvgIconSet(e,i){return this.addSvgIconSetInNamespace("",e,i)}addSvgIconSetLiteral(e,i){return this.addSvgIconSetLiteralInNamespace("",e,i)}addSvgIconSetInNamespace(e,i,r){return this._addSvgIconSetConfig(e,new Cr(i,null,r))}addSvgIconSetLiteralInNamespace(e,i,r){let o=this._sanitizer.sanitize(Bt.HTML,i);if(!o)throw OS(i);let s=hs(o);return this._addSvgIconSetConfig(e,new Cr("",s,r))}registerFontClassAlias(e,i=e){return this._fontCssClassesByAlias.set(e,i),this}classNameForFontAlias(e){return this._fontCssClassesByAlias.get(e)||e}setDefaultFontSetClass(...e){return this._defaultFontSetClass=e,this}getDefaultFontSetClass(){return this._defaultFontSetClass}getSvgIconFromUrl(e){let i=this._sanitizer.sanitize(Bt.RESOURCE_URL,e);if(!i)throw RS(e);let r=this._cachedIconsByUrl.get(i);return r?W(Mf(r)):this._loadSvgIconFromConfig(new Cr(e,null)).pipe(st(o=>this._cachedIconsByUrl.set(i,o)),Q(o=>Mf(o)))}getNamedSvgIcon(e,i=""){let r=NS(i,e),o=this._svgIconConfigs.get(r);if(o)return this._getSvgFromConfig(o);if(o=this._getIconConfigFromResolvers(i,e),o)return this._svgIconConfigs.set(r,o),this._getSvgFromConfig(o);let s=this._iconSetConfigs.get(i);return s?this._getSvgFromIconSetConfigs(e,s):Ao(AS(r))}ngOnDestroy(){this._resolvers=[],this._svgIconConfigs.clear(),this._iconSetConfigs.clear(),this._cachedIconsByUrl.clear()}_getSvgFromConfig(e){return e.svgText?W(Mf(this._svgElementFromConfig(e))):this._loadSvgIconFromConfig(e).pipe(Q(i=>Mf(i)))}_getSvgFromIconSetConfigs(e,i){let r=this._extractIconWithNameFromAnySet(e,i);if(r)return W(r);let o=i.filter(s=>!s.svgText).map(s=>this._loadSvgIconSetFromConfig(s).pipe(li(a=>{let l=`Loading icon set URL: ${this._sanitizer.sanitize(Bt.RESOURCE_URL,s.url)} failed: ${a.message}`;return this._errorHandler.handleError(new Error(l)),W(null)})));return Oo(o).pipe(Q(()=>{let s=this._extractIconWithNameFromAnySet(e,i);if(!s)throw AS(e);return s}))}_extractIconWithNameFromAnySet(e,i){for(let r=i.length-1;r>=0;r--){let o=i[r];if(o.svgText&&o.svgText.toString().indexOf(e)>-1){let s=this._svgElementFromConfig(o),a=this._extractSvgIconFromSet(s,e,o.options);if(a)return a}}return null}_loadSvgIconFromConfig(e){return this._fetchIcon(e).pipe(st(i=>e.svgText=i),Q(()=>this._svgElementFromConfig(e)))}_loadSvgIconSetFromConfig(e){return e.svgText?W(null):this._fetchIcon(e).pipe(st(i=>e.svgText=i))}_extractSvgIconFromSet(e,i,r){let o=e.querySelector(`[id="${i}"]`);if(!o)return null;let s=o.cloneNode(!0);if(s.removeAttribute("id"),s.nodeName.toLowerCase()==="svg")return this._setSvgAttributes(s,r);if(s.nodeName.toLowerCase()==="symbol")return this._setSvgAttributes(this._toSvgElement(s),r);let a=this._svgElementFromString(hs("<svg></svg>"));return a.appendChild(s),this._setSvgAttributes(a,r)}_svgElementFromString(e){let i=this._document.createElement("DIV");i.innerHTML=e;let r=i.querySelector("svg");if(!r)throw Error("<svg> tag not found");return r}_toSvgElement(e){let i=this._svgElementFromString(hs("<svg></svg>")),r=e.attributes;for(let o=0;o<r.length;o++){let{name:s,value:a}=r[o];s!=="id"&&i.setAttribute(s,a)}for(let o=0;o<e.childNodes.length;o++)e.childNodes[o].nodeType===this._document.ELEMENT_NODE&&i.appendChild(e.childNodes[o].cloneNode(!0));return i}_setSvgAttributes(e,i){return e.setAttribute("fit",""),e.setAttribute("height","100%"),e.setAttribute("width","100%"),e.setAttribute("preserveAspectRatio","xMidYMid meet"),e.setAttribute("focusable","false"),i&&i.viewBox&&e.setAttribute("viewBox",i.viewBox),e}_fetchIcon(e){let{url:i,options:r}=e,o=r?.withCredentials??!1;if(!this._httpClient)throw AL();if(i==null)throw Error(`Cannot fetch icon from URL "${i}".`);let s=this._sanitizer.sanitize(Bt.RESOURCE_URL,i);if(!s)throw RS(i);let a=this._inProgressUrlFetches.get(s);if(a)return a;let c=this._httpClient.get(s,{responseType:"text",withCredentials:o}).pipe(Q(l=>hs(l)),or(()=>this._inProgressUrlFetches.delete(s)),uc());return this._inProgressUrlFetches.set(s,c),c}_addSvgIconConfig(e,i,r){return this._svgIconConfigs.set(NS(e,i),r),this}_addSvgIconSetConfig(e,i){let r=this._iconSetConfigs.get(e);return r?r.push(i):this._iconSetConfigs.set(e,[i]),this}_svgElementFromConfig(e){if(!e.svgElement){let i=this._svgElementFromString(e.svgText);this._setSvgAttributes(i,e.options),e.svgElement=i}return e.svgElement}_getIconConfigFromResolvers(e,i){for(let r=0;r<this._resolvers.length;r++){let o=this._resolvers[r](i,e);if(o)return RL(o)?new Cr(o.url,null,o.options):new Cr(o,null)}}static \u0275fac=function(i){return new(i||t)(J(Wt,8),J(hl),J(K,8),J(rn))};static \u0275prov=C({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();function Mf(t){return t.cloneNode(!0)}function NS(t,n){return t+":"+n}function RL(t){return!!(t.url&&t.options)}var OL=["*"],NL=new y("MAT_ICON_DEFAULT_OPTIONS"),PL=new y("mat-icon-location",{providedIn:"root",factory:()=>{let t=d(K),n=t?t.location:null;return{getPathname:()=>n?n.pathname+n.search:""}}}),FS=["clip-path","color-profile","src","cursor","fill","filter","marker","marker-start","marker-mid","marker-end","mask","stroke"],FL=FS.map(t=>`[${t}]`).join(", "),LL=/^url\(['"]?#(.*?)['"]?\)$/,wt=(()=>{class t{_elementRef=d(L);_iconRegistry=d(PS);_location=d(PL);_errorHandler=d(rn);_defaultColor;get color(){return this._color||this._defaultColor}set color(e){this._color=e}_color;inline=!1;get svgIcon(){return this._svgIcon}set svgIcon(e){e!==this._svgIcon&&(e?this._updateSvgIcon(e):this._svgIcon&&this._clearSvgElement(),this._svgIcon=e)}_svgIcon;get fontSet(){return this._fontSet}set fontSet(e){let i=this._cleanupFontValue(e);i!==this._fontSet&&(this._fontSet=i,this._updateFontIconClasses())}_fontSet;get fontIcon(){return this._fontIcon}set fontIcon(e){let i=this._cleanupFontValue(e);i!==this._fontIcon&&(this._fontIcon=i,this._updateFontIconClasses())}_fontIcon;_previousFontSetClass=[];_previousFontIconClass;_svgName=null;_svgNamespace=null;_previousPath;_elementsWithExternalReferences;_currentIconFetch=ge.EMPTY;constructor(){let e=d(new Kn("aria-hidden"),{optional:!0}),i=d(NL,{optional:!0});i&&(i.color&&(this.color=this._defaultColor=i.color),i.fontSet&&(this.fontSet=i.fontSet)),e||this._elementRef.nativeElement.setAttribute("aria-hidden","true")}_splitIconName(e){if(!e)return["",""];let i=e.split(":");switch(i.length){case 1:return["",i[0]];case 2:return i;default:throw Error(`Invalid icon name: "${e}"`)}}ngOnInit(){this._updateFontIconClasses()}ngAfterViewChecked(){let e=this._elementsWithExternalReferences;if(e&&e.size){let i=this._location.getPathname();i!==this._previousPath&&(this._previousPath=i,this._prependPathToReferences(i))}}ngOnDestroy(){this._currentIconFetch.unsubscribe(),this._elementsWithExternalReferences&&this._elementsWithExternalReferences.clear()}_usingFontIcon(){return!this.svgIcon}_setSvgElement(e){this._clearSvgElement();let i=this._location.getPathname();this._previousPath=i,this._cacheChildrenWithExternalReferences(e),this._prependPathToReferences(i),this._elementRef.nativeElement.appendChild(e)}_clearSvgElement(){let e=this._elementRef.nativeElement,i=e.childNodes.length;for(this._elementsWithExternalReferences&&this._elementsWithExternalReferences.clear();i--;){let r=e.childNodes[i];(r.nodeType!==1||r.nodeName.toLowerCase()==="svg")&&r.remove()}}_updateFontIconClasses(){if(!this._usingFontIcon())return;let e=this._elementRef.nativeElement,i=(this.fontSet?this._iconRegistry.classNameForFontAlias(this.fontSet).split(/ +/):this._iconRegistry.getDefaultFontSetClass()).filter(r=>r.length>0);this._previousFontSetClass.forEach(r=>e.classList.remove(r)),i.forEach(r=>e.classList.add(r)),this._previousFontSetClass=i,this.fontIcon!==this._previousFontIconClass&&!i.includes("mat-ligature-font")&&(this._previousFontIconClass&&e.classList.remove(this._previousFontIconClass),this.fontIcon&&e.classList.add(this.fontIcon),this._previousFontIconClass=this.fontIcon)}_cleanupFontValue(e){return typeof e=="string"?e.trim().split(" ")[0]:e}_prependPathToReferences(e){let i=this._elementsWithExternalReferences;i&&i.forEach((r,o)=>{r.forEach(s=>{o.setAttribute(s.name,`url('${e}#${s.value}')`)})})}_cacheChildrenWithExternalReferences(e){let i=e.querySelectorAll(FL),r=this._elementsWithExternalReferences=this._elementsWithExternalReferences||new Map;for(let o=0;o<i.length;o++)FS.forEach(s=>{let a=i[o],c=a.getAttribute(s),l=c?c.match(LL):null;if(l){let u=r.get(a);u||(u=[],r.set(a,u)),u.push({name:s,value:l[1]})}})}_updateSvgIcon(e){if(this._svgNamespace=null,this._svgName=null,this._currentIconFetch.unsubscribe(),e){let[i,r]=this._splitIconName(e);i&&(this._svgNamespace=i),r&&(this._svgName=r),this._currentIconFetch=this._iconRegistry.getNamedSvgIcon(r,i).pipe(ut(1)).subscribe(o=>this._setSvgElement(o),o=>{let s=`Error retrieving icon ${i}:${r}! ${o.message}`;this._errorHandler.handleError(new Error(s))})}}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=E({type:t,selectors:[["mat-icon"]],hostAttrs:["role","img",1,"mat-icon","notranslate"],hostVars:10,hostBindings:function(i,r){i&2&&(q("data-mat-icon-type",r._usingFontIcon()?"font":"svg")("data-mat-icon-name",r._svgName||r.fontIcon)("data-mat-icon-namespace",r._svgNamespace||r.fontSet)("fontIcon",r._usingFontIcon()?r.fontIcon:null),Tt(r.color?"mat-"+r.color:""),z("mat-icon-inline",r.inline)("mat-icon-no-color",r.color!=="primary"&&r.color!=="accent"&&r.color!=="warn"))},inputs:{color:"color",inline:[2,"inline","inline",B],svgIcon:"svgIcon",fontSet:"fontSet",fontIcon:"fontIcon"},exportAs:["matIcon"],ngContentSelectors:OL,decls:1,vars:0,template:function(i,r){i&1&&(Ce(),re(0))},styles:[`mat-icon, mat-icon.mat-primary, mat-icon.mat-accent, mat-icon.mat-warn {
  color: var(--mat-icon-color, inherit);
}

.mat-icon {
  -webkit-user-select: none;
  user-select: none;
  background-repeat: no-repeat;
  display: inline-block;
  fill: currentColor;
  height: 24px;
  width: 24px;
  overflow: hidden;
}
.mat-icon.mat-icon-inline {
  font-size: inherit;
  height: inherit;
  line-height: inherit;
  width: inherit;
}
.mat-icon.mat-ligature-font[fontIcon]::before {
  content: attr(fontIcon);
}

[dir=rtl] .mat-icon-rtl-mirror {
  transform: scale(-1, 1);
}

.mat-form-field:not(.mat-form-field-appearance-legacy) .mat-form-field-prefix .mat-icon,
.mat-form-field:not(.mat-form-field-appearance-legacy) .mat-form-field-suffix .mat-icon {
  display: block;
}
.mat-form-field:not(.mat-form-field-appearance-legacy) .mat-form-field-prefix .mat-icon-button .mat-icon,
.mat-form-field:not(.mat-form-field-appearance-legacy) .mat-form-field-suffix .mat-icon-button .mat-icon {
  margin: auto;
}
`],encapsulation:2,changeDetection:0})}return t})(),LS=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=ae({type:t});static \u0275inj=oe({imports:[Yt]})}return t})();var jS="useandom-26T198340PX75pxJACKVERYMINDBUSHWOLF_GQZbfghjklqvwyzrict";var BS=(t=21)=>{let n="",e=crypto.getRandomValues(new Uint8Array(t|=0));for(;t--;)n+=jS[e[t]&63];return n};var Tf=class{id=BS();items=[];deliveryMethodId;paymentIntentId;clientSecret};var rt=class t{baseUrl=oo.apiUrl;http=d(Wt);cart=M(null);selectedDeliveryMethod=M(null);itemCount=xt(()=>this.cart()?.items.reduce((n,e)=>n+e.quantity,0)??0);totals=xt(()=>{let n=this.cart(),e=this.selectedDeliveryMethod();if(!n)return null;let i=n.items.reduce((s,a)=>s+a.price*a.quantity,0),r=e?e.price:0,o=0;return{subtotal:i,shipping:r,discount:o,total:i+r-o}});getCart(n){return this.http.get(this.baseUrl+"cart?id="+n).pipe(Q(e=>(this.cart.set(e),e)))}setCart(n){this.http.post(this.baseUrl+"cart",n).subscribe({next:e=>this.cart.set(e),error:e=>console.error(e)})}addItemToCart(n,e=1){let i=this.cart()??this.createCart();this.isProduct(n)&&(n=this.mapProductItemToCartItem(n)),i.items=this.addOrUpdateItem(i.items,n,e),this.setCart(i)}removeItemFromCart(n,e=1){let i=this.cart();if(!i)return;let r=i.items.findIndex(o=>o.productId===n);r!==-1&&(i.items[r].quantity>e?i.items[r].quantity-=e:i.items.splice(r,1),i.items.length===0?this.deleteCart():this.setCart(i))}deleteCart(){this.http.delete(this.baseUrl+"cart?id="+this.cart()?.id).subscribe({next:()=>{localStorage.removeItem("cart_id"),this.cart.set(null)},error:n=>console.error(n)})}addOrUpdateItem(n,e,i){let r=n.findIndex(o=>o.productId===e.productId);return r===-1?(e.quantity=i,n.push(e)):n[r].quantity+=i,n}mapProductItemToCartItem(n){return{productId:n.id,productName:n.name,price:n.price,quantity:0,pictureUrl:n.pictureUrl,artist:n.artist,genre:n.genre,label:n.label}}isProduct(n){return"id"in n}createCart(){let n=new Tf;return localStorage.setItem("cart_id",n.id),n}static \u0275fac=function(e){return new(e||t)};static \u0275prov=C({token:t,factory:t.\u0275fac,providedIn:"root"})};function jL(t,n){if(t&1){let e=Mt();f(0,"mat-card",0),O(1,"img",1),f(2,"mat-card-content",2)(3,"h2",3),_(4),h(),f(5,"p"),_(6),h(),f(7,"p",4),_(8),Se(9,"currency"),h()(),f(10,"mat-card-actions")(11,"button",5),T("click",function(r){Te(e);let o=S();return r.stopPropagation(),Ae(o.cartService.addItemToCart(o.product))}),f(12,"mat-icon"),_(13,"add_shopping_cart"),h(),_(14," Add to cart "),h()()()}if(t&2){let e=S();D("routerLink",Ln("/shop/",e.product.id)),p(),D("src",an(e.product.pictureUrl),Qn)("alt",Ln("image of ",e.product.name)),p(3),V(e.product.name),p(2),V(e.product.artist),p(2),V(Oe(9,9,e.product.price))}}var Af=class t{product;cartService=d(rt);static \u0275fac=function(e){return new(e||t)};static \u0275cmp=E({type:t,selectors:[["app-product-item"]],inputs:{product:"product"},decls:1,vars:1,consts:[["appearence","raised",1,"product-card",3,"routerLink"],[1,"aspect-square","w-full","object-cover",3,"src","alt"],[1,"mt-2"],[1,"font-semibold"],[1,"font-light"],["mat-stroked-button","",1,"w-full",3,"click"]],template:function(e,i){e&1&&N(0,jL,15,11,"mat-card",0),e&2&&P(i.product?0:-1)},dependencies:[Zi,QE,XE,Pe,wt,dt,Ut],styles:[".product-card[_ngcontent-%COMP%]{transition:transform .2s,box-shadow .2s}.product-card[_ngcontent-%COMP%]:hover{transform:translateY(-5px);box-shadow:0 4px 8px #0003;cursor:pointer}"]})};var BL=20,Ta=(()=>{class t{_ngZone=d(H);_platform=d(ye);_renderer=d(Ot).createRenderer(null,null);_cleanupGlobalListener;constructor(){}_scrolled=new I;_scrolledCount=0;scrollContainers=new Map;register(e){this.scrollContainers.has(e)||this.scrollContainers.set(e,e.elementScrolled().subscribe(()=>this._scrolled.next(e)))}deregister(e){let i=this.scrollContainers.get(e);i&&(i.unsubscribe(),this.scrollContainers.delete(e))}scrolled(e=BL){return this._platform.isBrowser?new ce(i=>{this._cleanupGlobalListener||(this._cleanupGlobalListener=this._ngZone.runOutsideAngular(()=>this._renderer.listen("document","scroll",()=>this._scrolled.next())));let r=e>0?this._scrolled.pipe(Qd(e)).subscribe(i):this._scrolled.subscribe(i);return this._scrolledCount++,()=>{r.unsubscribe(),this._scrolledCount--,this._scrolledCount||(this._cleanupGlobalListener?.(),this._cleanupGlobalListener=void 0)}}):W()}ngOnDestroy(){this._cleanupGlobalListener?.(),this._cleanupGlobalListener=void 0,this.scrollContainers.forEach((e,i)=>this.deregister(i)),this._scrolled.complete()}ancestorScrolled(e,i){let r=this.getAncestorScrollContainers(e);return this.scrolled(i).pipe(De(o=>!o||r.indexOf(o)>-1))}getAncestorScrollContainers(e){let i=[];return this.scrollContainers.forEach((r,o)=>{this._scrollableContainsElement(o,e)&&i.push(o)}),i}_scrollableContainsElement(e,i){let r=xn(i),o=e.getElementRef().nativeElement;do if(r==o)return!0;while(r=r.parentElement);return!1}static \u0275fac=function(i){return new(i||t)};static \u0275prov=C({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();var VL=20,ao=(()=>{class t{_platform=d(ye);_listeners;_viewportSize=null;_change=new I;_document=d(K);constructor(){let e=d(H),i=d(Ot).createRenderer(null,null);e.runOutsideAngular(()=>{if(this._platform.isBrowser){let r=o=>this._change.next(o);this._listeners=[i.listen("window","resize",r),i.listen("window","orientationchange",r)]}this.change().subscribe(()=>this._viewportSize=null)})}ngOnDestroy(){this._listeners?.forEach(e=>e()),this._change.complete()}getViewportSize(){this._viewportSize||this._updateViewportSize();let e={width:this._viewportSize.width,height:this._viewportSize.height};return this._platform.isBrowser||(this._viewportSize=null),e}getViewportRect(){let e=this.getViewportScrollPosition(),{width:i,height:r}=this.getViewportSize();return{top:e.top,left:e.left,bottom:e.top+r,right:e.left+i,height:r,width:i}}getViewportScrollPosition(){if(!this._platform.isBrowser)return{top:0,left:0};let e=this._document,i=this._getWindow(),r=e.documentElement,o=r.getBoundingClientRect(),s=-o.top||e.body?.scrollTop||i.scrollY||r.scrollTop||0,a=-o.left||e.body?.scrollLeft||i.scrollX||r.scrollLeft||0;return{top:s,left:a}}change(e=VL){return e>0?this._change.pipe(Qd(e)):this._change}_getWindow(){return this._document.defaultView||window}_updateViewportSize(){let e=this._getWindow();this._viewportSize=this._platform.isBrowser?{width:e.innerWidth,height:e.innerHeight}:{width:0,height:0}}static \u0275fac=function(i){return new(i||t)};static \u0275prov=C({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();var Gl=class{_attachedHost=null;attach(n){return this._attachedHost=n,n.attach(this)}detach(){let n=this._attachedHost;n!=null&&(this._attachedHost=null,n.detach())}get isAttached(){return this._attachedHost!=null}setAttachedHost(n){this._attachedHost=n}},Ci=class extends Gl{component;viewContainerRef;injector;projectableNodes;bindings;constructor(n,e,i,r,o){super(),this.component=n,this.viewContainerRef=e,this.injector=i,this.projectableNodes=r,this.bindings=o||null}},Un=class extends Gl{templateRef;viewContainerRef;context;injector;constructor(n,e,i,r){super(),this.templateRef=n,this.viewContainerRef=e,this.context=i,this.injector=r}get origin(){return this.templateRef.elementRef}attach(n,e=this.context){return this.context=e,super.attach(n)}detach(){return this.context=void 0,super.detach()}},Ab=class extends Gl{element;constructor(n){super(),this.element=n instanceof L?n.nativeElement:n}},co=class{_attachedPortal=null;_disposeFn=null;_isDisposed=!1;hasAttached(){return!!this._attachedPortal}attach(n){if(n instanceof Ci)return this._attachedPortal=n,this.attachComponentPortal(n);if(n instanceof Un)return this._attachedPortal=n,this.attachTemplatePortal(n);if(this.attachDomPortal&&n instanceof Ab)return this._attachedPortal=n,this.attachDomPortal(n)}attachDomPortal=null;detach(){this._attachedPortal&&(this._attachedPortal.setAttachedHost(null),this._attachedPortal=null),this._invokeDisposeFn()}dispose(){this.hasAttached()&&this.detach(),this._invokeDisposeFn(),this._isDisposed=!0}setDisposeFn(n){this._disposeFn=n}_invokeDisposeFn(){this._disposeFn&&(this._disposeFn(),this._disposeFn=null)}},Wl=class extends co{outletElement;_appRef;_defaultInjector;constructor(n,e,i){super(),this.outletElement=n,this._appRef=e,this._defaultInjector=i}attachComponentPortal(n){let e;if(n.viewContainerRef){let i=n.injector||n.viewContainerRef.injector,r=i.get(zi,null,{optional:!0})||void 0;e=n.viewContainerRef.createComponent(n.component,{index:n.viewContainerRef.length,injector:i,ngModuleRef:r,projectableNodes:n.projectableNodes||void 0,bindings:n.bindings||void 0}),this.setDisposeFn(()=>e.destroy())}else{let i=this._appRef,r=n.injector||this._defaultInjector||X.NULL,o=r.get(He,i.injector);e=Dm(n.component,{elementInjector:r,environmentInjector:o,projectableNodes:n.projectableNodes||void 0,bindings:n.bindings||void 0}),i.attachView(e.hostView),this.setDisposeFn(()=>{i.viewCount>0&&i.detachView(e.hostView),e.destroy()})}return this.outletElement.appendChild(this._getComponentRootNode(e)),this._attachedPortal=n,e}attachTemplatePortal(n){let e=n.viewContainerRef,i=e.createEmbeddedView(n.templateRef,n.context,{injector:n.injector});return i.rootNodes.forEach(r=>this.outletElement.appendChild(r)),i.detectChanges(),this.setDisposeFn(()=>{let r=e.indexOf(i);r!==-1&&e.remove(r)}),this._attachedPortal=n,i}attachDomPortal=n=>{let e=n.element;e.parentNode;let i=this.outletElement.ownerDocument.createComment("dom-portal");e.parentNode.insertBefore(i,e),this.outletElement.appendChild(e),this._attachedPortal=n,super.setDisposeFn(()=>{i.parentNode&&i.parentNode.replaceChild(e,i)})};dispose(){super.dispose(),this.outletElement.remove()}_getComponentRootNode(n){return n.hostView.rootNodes[0]}};var Ki=(()=>{class t extends co{_moduleRef=d(zi,{optional:!0});_document=d(K);_viewContainerRef=d(Vt);_isInitialized=!1;_attachedRef=null;constructor(){super()}get portal(){return this._attachedPortal}set portal(e){this.hasAttached()&&!e&&!this._isInitialized||(this.hasAttached()&&super.detach(),e&&super.attach(e),this._attachedPortal=e||null)}attached=new U;get attachedRef(){return this._attachedRef}ngOnInit(){this._isInitialized=!0}ngOnDestroy(){super.dispose(),this._attachedRef=this._attachedPortal=null}attachComponentPortal(e){e.setAttachedHost(this);let i=e.viewContainerRef!=null?e.viewContainerRef:this._viewContainerRef,r=i.createComponent(e.component,{index:i.length,injector:e.injector||i.injector,projectableNodes:e.projectableNodes||void 0,ngModuleRef:this._moduleRef||void 0,bindings:e.bindings||void 0});return i!==this._viewContainerRef&&this._getRootNode().appendChild(r.hostView.rootNodes[0]),super.setDisposeFn(()=>r.destroy()),this._attachedPortal=e,this._attachedRef=r,this.attached.emit(r),r}attachTemplatePortal(e){e.setAttachedHost(this);let i=this._viewContainerRef.createEmbeddedView(e.templateRef,e.context,{injector:e.injector});return super.setDisposeFn(()=>this._viewContainerRef.clear()),this._attachedPortal=e,this._attachedRef=i,this.attached.emit(i),i}attachDomPortal=e=>{let i=e.element;i.parentNode;let r=this._document.createComment("dom-portal");e.setAttachedHost(this),i.parentNode.insertBefore(r,i),this._getRootNode().appendChild(i),this._attachedPortal=e,super.setDisposeFn(()=>{r.parentNode&&r.parentNode.replaceChild(i,r)})};_getRootNode(){let e=this._viewContainerRef.element.nativeElement;return e.nodeType===e.ELEMENT_NODE?e:e.parentNode}static \u0275fac=function(i){return new(i||t)};static \u0275dir=A({type:t,selectors:[["","cdkPortalOutlet",""]],inputs:{portal:[0,"cdkPortalOutlet","portal"]},outputs:{attached:"attached"},exportAs:["cdkPortalOutlet"],features:[fe]})}return t})(),Aa=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=ae({type:t});static \u0275inj=oe({})}return t})();var VS=bS();function ql(t){return new Rf(t.get(ao),t.get(K))}var Rf=class{_viewportRuler;_previousHTMLStyles={top:"",left:""};_previousScrollPosition;_isEnabled=!1;_document;constructor(n,e){this._viewportRuler=n,this._document=e}attach(){}enable(){if(this._canBeEnabled()){let n=this._document.documentElement;this._previousScrollPosition=this._viewportRuler.getViewportScrollPosition(),this._previousHTMLStyles.left=n.style.left||"",this._previousHTMLStyles.top=n.style.top||"",n.style.left=At(-this._previousScrollPosition.left),n.style.top=At(-this._previousScrollPosition.top),n.classList.add("cdk-global-scrollblock"),this._isEnabled=!0}}disable(){if(this._isEnabled){let n=this._document.documentElement,e=this._document.body,i=n.style,r=e.style,o=i.scrollBehavior||"",s=r.scrollBehavior||"";this._isEnabled=!1,i.left=this._previousHTMLStyles.left,i.top=this._previousHTMLStyles.top,n.classList.remove("cdk-global-scrollblock"),VS&&(i.scrollBehavior=r.scrollBehavior="auto"),window.scroll(this._previousScrollPosition.left,this._previousScrollPosition.top),VS&&(i.scrollBehavior=o,r.scrollBehavior=s)}}_canBeEnabled(){if(this._document.documentElement.classList.contains("cdk-global-scrollblock")||this._isEnabled)return!1;let e=this._document.documentElement,i=this._viewportRuler.getViewportSize();return e.scrollHeight>i.height||e.scrollWidth>i.width}};var Of=class{enable(){}disable(){}attach(){}};function Rb(t,n){return n.some(e=>{let i=t.bottom<e.top,r=t.top>e.bottom,o=t.right<e.left,s=t.left>e.right;return i||r||o||s})}function HS(t,n){return n.some(e=>{let i=t.top<e.top,r=t.bottom>e.bottom,o=t.left<e.left,s=t.right>e.right;return i||r||o||s})}function lo(t,n){return new Nf(t.get(Ta),t.get(ao),t.get(H),n)}var Nf=class{_scrollDispatcher;_viewportRuler;_ngZone;_config;_scrollSubscription=null;_overlayRef;constructor(n,e,i,r){this._scrollDispatcher=n,this._viewportRuler=e,this._ngZone=i,this._config=r}attach(n){this._overlayRef,this._overlayRef=n}enable(){if(!this._scrollSubscription){let n=this._config?this._config.scrollThrottle:0;this._scrollSubscription=this._scrollDispatcher.scrolled(n).subscribe(()=>{if(this._overlayRef.updatePosition(),this._config&&this._config.autoClose){let e=this._overlayRef.overlayElement.getBoundingClientRect(),{width:i,height:r}=this._viewportRuler.getViewportSize();Rb(e,[{width:i,height:r,bottom:r,right:i,top:0,left:0}])&&(this.disable(),this._ngZone.run(()=>this._overlayRef.detach()))}})}}disable(){this._scrollSubscription&&(this._scrollSubscription.unsubscribe(),this._scrollSubscription=null)}detach(){this.disable(),this._overlayRef=null}};var wi=class{positionStrategy;scrollStrategy=new Of;panelClass="";hasBackdrop=!1;backdropClass="cdk-overlay-dark-backdrop";disableAnimations;width;height;minWidth;minHeight;maxWidth;maxHeight;direction;disposeOnNavigation=!1;usePopover;eventPredicate;constructor(n){if(n){let e=Object.keys(n);for(let i of e)n[i]!==void 0&&(this[i]=n[i])}}};var Pf=class{connectionPair;scrollableViewProperties;constructor(n,e){this.connectionPair=n,this.scrollableViewProperties=e}};var qS=(()=>{class t{_attachedOverlays=[];_document=d(K);_isAttached=!1;constructor(){}ngOnDestroy(){this.detach()}add(e){this.remove(e),this._attachedOverlays.push(e)}remove(e){let i=this._attachedOverlays.indexOf(e);i>-1&&this._attachedOverlays.splice(i,1),this._attachedOverlays.length===0&&this.detach()}canReceiveEvent(e,i,r){return r.observers.length<1?!1:e.eventPredicate?e.eventPredicate(i):!0}static \u0275fac=function(i){return new(i||t)};static \u0275prov=C({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})(),YS=(()=>{class t extends qS{_ngZone=d(H);_renderer=d(Ot).createRenderer(null,null);_cleanupKeydown;add(e){super.add(e),this._isAttached||(this._ngZone.runOutsideAngular(()=>{this._cleanupKeydown=this._renderer.listen("body","keydown",this._keydownListener)}),this._isAttached=!0)}detach(){this._isAttached&&(this._cleanupKeydown?.(),this._isAttached=!1)}_keydownListener=e=>{let i=this._attachedOverlays;for(let r=i.length-1;r>-1;r--){let o=i[r];if(this.canReceiveEvent(o,e,o._keydownEvents)){this._ngZone.run(()=>o._keydownEvents.next(e));break}}};static \u0275fac=(()=>{let e;return function(r){return(e||(e=ct(t)))(r||t)}})();static \u0275prov=C({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})(),ZS=(()=>{class t extends qS{_platform=d(ye);_ngZone=d(H);_renderer=d(Ot).createRenderer(null,null);_cursorOriginalValue;_cursorStyleIsSet=!1;_pointerDownEventTarget=null;_cleanups;add(e){if(super.add(e),!this._isAttached){let i=this._document.body,r={capture:!0},o=this._renderer;this._cleanups=this._ngZone.runOutsideAngular(()=>[o.listen(i,"pointerdown",this._pointerDownListener,r),o.listen(i,"click",this._clickListener,r),o.listen(i,"auxclick",this._clickListener,r),o.listen(i,"contextmenu",this._clickListener,r)]),this._platform.IOS&&!this._cursorStyleIsSet&&(this._cursorOriginalValue=i.style.cursor,i.style.cursor="pointer",this._cursorStyleIsSet=!0),this._isAttached=!0}}detach(){this._isAttached&&(this._cleanups?.forEach(e=>e()),this._cleanups=void 0,this._platform.IOS&&this._cursorStyleIsSet&&(this._document.body.style.cursor=this._cursorOriginalValue,this._cursorStyleIsSet=!1),this._isAttached=!1)}_pointerDownListener=e=>{this._pointerDownEventTarget=Kt(e)};_clickListener=e=>{let i=Kt(e),r=e.type==="click"&&this._pointerDownEventTarget?this._pointerDownEventTarget:i;this._pointerDownEventTarget=null;let o=this._attachedOverlays.slice();for(let s=o.length-1;s>-1;s--){let a=o[s],c=a._outsidePointerEvents;if(!(!a.hasAttached()||!this.canReceiveEvent(a,e,c))){if(US(a.overlayElement,i)||US(a.overlayElement,r))break;this._ngZone?this._ngZone.run(()=>c.next(e)):c.next(e)}}};static \u0275fac=(()=>{let e;return function(r){return(e||(e=ct(t)))(r||t)}})();static \u0275prov=C({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();function US(t,n){let e=typeof ShadowRoot<"u"&&ShadowRoot,i=n;for(;i;){if(i===t)return!0;i=e&&i instanceof ShadowRoot?i.host:i.parentNode}return!1}var QS=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275cmp=E({type:t,selectors:[["ng-component"]],hostAttrs:["cdk-overlay-style-loader",""],decls:0,vars:0,template:function(i,r){},styles:[`.cdk-overlay-container, .cdk-global-overlay-wrapper {
  pointer-events: none;
  top: 0;
  left: 0;
  height: 100%;
  width: 100%;
}

.cdk-overlay-container {
  position: fixed;
}
@layer cdk-overlay {
  .cdk-overlay-container {
    z-index: 1000;
  }
}
.cdk-overlay-container:empty {
  display: none;
}

.cdk-global-overlay-wrapper {
  display: flex;
  position: absolute;
}
@layer cdk-overlay {
  .cdk-global-overlay-wrapper {
    z-index: 1000;
  }
}

.cdk-overlay-pane {
  position: absolute;
  pointer-events: auto;
  box-sizing: border-box;
  display: flex;
  max-width: 100%;
  max-height: 100%;
}
@layer cdk-overlay {
  .cdk-overlay-pane {
    z-index: 1000;
  }
}

.cdk-overlay-backdrop {
  position: absolute;
  top: 0;
  bottom: 0;
  left: 0;
  right: 0;
  pointer-events: auto;
  -webkit-tap-highlight-color: transparent;
  opacity: 0;
  touch-action: manipulation;
}
@layer cdk-overlay {
  .cdk-overlay-backdrop {
    z-index: 1000;
    transition: opacity 400ms cubic-bezier(0.25, 0.8, 0.25, 1);
  }
}
@media (prefers-reduced-motion) {
  .cdk-overlay-backdrop {
    transition-duration: 1ms;
  }
}

.cdk-overlay-backdrop-showing {
  opacity: 1;
}
@media (forced-colors: active) {
  .cdk-overlay-backdrop-showing {
    opacity: 0.6;
  }
}

@layer cdk-overlay {
  .cdk-overlay-dark-backdrop {
    background: rgba(0, 0, 0, 0.32);
  }
}

.cdk-overlay-transparent-backdrop {
  transition: visibility 1ms linear, opacity 1ms linear;
  visibility: hidden;
  opacity: 1;
}
.cdk-overlay-transparent-backdrop.cdk-overlay-backdrop-showing, .cdk-high-contrast-active .cdk-overlay-transparent-backdrop {
  opacity: 0;
  visibility: visible;
}

.cdk-overlay-backdrop-noop-animation {
  transition: none;
}

.cdk-overlay-connected-position-bounding-box {
  position: absolute;
  display: flex;
  flex-direction: column;
  min-width: 1px;
  min-height: 1px;
}
@layer cdk-overlay {
  .cdk-overlay-connected-position-bounding-box {
    z-index: 1000;
  }
}

.cdk-global-scrollblock {
  position: fixed;
  width: 100%;
  overflow-y: scroll;
}

.cdk-overlay-popover {
  background: none;
  border: none;
  padding: 0;
  outline: 0;
  overflow: visible;
  position: fixed;
  pointer-events: none;
  white-space: normal;
  color: inherit;
  text-decoration: none;
  width: 100%;
  height: 100%;
  inset: auto;
  top: 0;
  left: 0;
}
.cdk-overlay-popover::backdrop {
  display: none;
}
.cdk-overlay-popover .cdk-overlay-backdrop {
  position: fixed;
  z-index: auto;
}
`],encapsulation:2,changeDetection:0})}return t})(),jf=(()=>{class t{_platform=d(ye);_containerElement;_document=d(K);_styleLoader=d(qe);constructor(){}ngOnDestroy(){this._containerElement?.remove()}getContainerElement(){return this._loadStyles(),this._containerElement||this._createContainer(),this._containerElement}_createContainer(){let e="cdk-overlay-container";if(this._platform.isBrowser||Db()){let r=this._document.querySelectorAll(`.${e}[platform="server"], .${e}[platform="test"]`);for(let o=0;o<r.length;o++)r[o].remove()}let i=this._document.createElement("div");i.classList.add(e),Db()?i.setAttribute("platform","test"):this._platform.isBrowser||i.setAttribute("platform","server"),this._document.body.appendChild(i),this._containerElement=i}_loadStyles(){this._styleLoader.load(QS)}static \u0275fac=function(i){return new(i||t)};static \u0275prov=C({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})(),Ob=class{_renderer;_ngZone;element;_cleanupClick;_cleanupTransitionEnd;_fallbackTimeout;constructor(n,e,i,r){this._renderer=e,this._ngZone=i,this.element=n.createElement("div"),this.element.classList.add("cdk-overlay-backdrop"),this._cleanupClick=e.listen(this.element,"click",r)}detach(){this._ngZone.runOutsideAngular(()=>{let n=this.element;clearTimeout(this._fallbackTimeout),this._cleanupTransitionEnd?.(),this._cleanupTransitionEnd=this._renderer.listen(n,"transitionend",this.dispose),this._fallbackTimeout=setTimeout(this.dispose,500),n.style.pointerEvents="none",n.classList.remove("cdk-overlay-backdrop-showing")})}dispose=()=>{clearTimeout(this._fallbackTimeout),this._cleanupClick?.(),this._cleanupTransitionEnd?.(),this._cleanupClick=this._cleanupTransitionEnd=this._fallbackTimeout=void 0,this.element.remove()}};function Nb(t){return t&&t.nodeType===1}var Ra=class{_portalOutlet;_host;_pane;_config;_ngZone;_keyboardDispatcher;_document;_location;_outsideClickDispatcher;_animationsDisabled;_injector;_renderer;_backdropClick=new I;_attachments=new I;_detachments=new I;_positionStrategy;_scrollStrategy;_locationChanges=ge.EMPTY;_backdropRef=null;_detachContentMutationObserver;_detachContentAfterRenderRef;_disposed=!1;_previousHostParent;_keydownEvents=new I;_outsidePointerEvents=new I;_afterNextRenderRef;constructor(n,e,i,r,o,s,a,c,l,u=!1,m,g){this._portalOutlet=n,this._host=e,this._pane=i,this._config=r,this._ngZone=o,this._keyboardDispatcher=s,this._document=a,this._location=c,this._outsideClickDispatcher=l,this._animationsDisabled=u,this._injector=m,this._renderer=g,r.scrollStrategy&&(this._scrollStrategy=r.scrollStrategy,this._scrollStrategy.attach(this)),this._positionStrategy=r.positionStrategy}get overlayElement(){return this._pane}get backdropElement(){return this._backdropRef?.element||null}get hostElement(){return this._host}get eventPredicate(){return this._config?.eventPredicate||null}attach(n){if(this._disposed)return null;this._attachHost();let e=this._portalOutlet.attach(n);return this._positionStrategy?.attach(this),this._updateStackingOrder(),this._updateElementSize(),this._updateElementDirection(),this._scrollStrategy&&this._scrollStrategy.enable(),this._afterNextRenderRef?.destroy(),this._afterNextRenderRef=bt(()=>{this.hasAttached()&&this.updatePosition()},{injector:this._injector}),this._togglePointerEvents(!0),this._config.hasBackdrop&&this._attachBackdrop(),this._config.panelClass&&this._toggleClasses(this._pane,this._config.panelClass,!0),this._attachments.next(),this._completeDetachContent(),this._keyboardDispatcher.add(this),this._config.disposeOnNavigation&&(this._locationChanges=this._location.subscribe(()=>this.dispose())),this._outsideClickDispatcher.add(this),typeof e?.onDestroy=="function"&&e.onDestroy(()=>{this.hasAttached()&&this._ngZone.runOutsideAngular(()=>Promise.resolve().then(()=>this.detach()))}),e}detach(){if(!this.hasAttached())return;this.detachBackdrop(),this._togglePointerEvents(!1),this._positionStrategy&&this._positionStrategy.detach&&this._positionStrategy.detach(),this._scrollStrategy&&this._scrollStrategy.disable();let n=this._portalOutlet.detach();return this._detachments.next(),this._completeDetachContent(),this._keyboardDispatcher.remove(this),this._detachContentWhenEmpty(),this._locationChanges.unsubscribe(),this._outsideClickDispatcher.remove(this),n}dispose(){if(this._disposed)return;let n=this.hasAttached();this._positionStrategy&&this._positionStrategy.dispose(),this._disposeScrollStrategy(),this._backdropRef?.dispose(),this._locationChanges.unsubscribe(),this._keyboardDispatcher.remove(this),this._portalOutlet.dispose(),this._attachments.complete(),this._backdropClick.complete(),this._keydownEvents.complete(),this._outsidePointerEvents.complete(),this._outsideClickDispatcher.remove(this),this._host?.remove(),this._afterNextRenderRef?.destroy(),this._previousHostParent=this._pane=this._host=this._backdropRef=null,n&&this._detachments.next(),this._detachments.complete(),this._completeDetachContent(),this._disposed=!0}hasAttached(){return this._portalOutlet.hasAttached()}backdropClick(){return this._backdropClick}attachments(){return this._attachments}detachments(){return this._detachments}keydownEvents(){return this._keydownEvents}outsidePointerEvents(){return this._outsidePointerEvents}getConfig(){return this._config}updatePosition(){this._positionStrategy&&this._positionStrategy.apply()}updatePositionStrategy(n){n!==this._positionStrategy&&(this._positionStrategy&&this._positionStrategy.dispose(),this._positionStrategy=n,this.hasAttached()&&(n.attach(this),this.updatePosition()))}updateSize(n){this._config=b(b({},this._config),n),this._updateElementSize()}setDirection(n){this._config=Z(b({},this._config),{direction:n}),this._updateElementDirection()}addPanelClass(n){this._pane&&this._toggleClasses(this._pane,n,!0)}removePanelClass(n){this._pane&&this._toggleClasses(this._pane,n,!1)}getDirection(){let n=this._config.direction;return n?typeof n=="string"?n:n.value:"ltr"}updateScrollStrategy(n){n!==this._scrollStrategy&&(this._disposeScrollStrategy(),this._scrollStrategy=n,this.hasAttached()&&(n.attach(this),n.enable()))}_updateElementDirection(){this._host.setAttribute("dir",this.getDirection())}_updateElementSize(){if(!this._pane)return;let n=this._pane.style;n.width=At(this._config.width),n.height=At(this._config.height),n.minWidth=At(this._config.minWidth),n.minHeight=At(this._config.minHeight),n.maxWidth=At(this._config.maxWidth),n.maxHeight=At(this._config.maxHeight)}_togglePointerEvents(n){this._pane.style.pointerEvents=n?"":"none"}_attachHost(){if(!this._host.parentElement){let n=this._config.usePopover?this._positionStrategy?.getPopoverInsertionPoint?.():null;Nb(n)?n.after(this._host):n?.type==="parent"?n.element.appendChild(this._host):this._previousHostParent?.appendChild(this._host)}if(this._config.usePopover)try{this._host.showPopover()}catch{}}_attachBackdrop(){let n="cdk-overlay-backdrop-showing";this._backdropRef?.dispose(),this._backdropRef=new Ob(this._document,this._renderer,this._ngZone,e=>{this._backdropClick.next(e)}),this._animationsDisabled&&this._backdropRef.element.classList.add("cdk-overlay-backdrop-noop-animation"),this._config.backdropClass&&this._toggleClasses(this._backdropRef.element,this._config.backdropClass,!0),this._config.usePopover?this._host.prepend(this._backdropRef.element):this._host.parentElement.insertBefore(this._backdropRef.element,this._host),!this._animationsDisabled&&typeof requestAnimationFrame<"u"?this._ngZone.runOutsideAngular(()=>{requestAnimationFrame(()=>this._backdropRef?.element.classList.add(n))}):this._backdropRef.element.classList.add(n)}_updateStackingOrder(){!this._config.usePopover&&this._host.nextSibling&&this._host.parentNode.appendChild(this._host)}detachBackdrop(){this._animationsDisabled?(this._backdropRef?.dispose(),this._backdropRef=null):this._backdropRef?.detach()}_toggleClasses(n,e,i){let r=Ea(e||[]).filter(o=>!!o);r.length&&(i?n.classList.add(...r):n.classList.remove(...r))}_detachContentWhenEmpty(){let n=!1;try{this._detachContentAfterRenderRef=bt(()=>{n=!0,this._detachContent()},{injector:this._injector})}catch(e){if(n)throw e;this._detachContent()}globalThis.MutationObserver&&this._pane&&(this._detachContentMutationObserver||=new globalThis.MutationObserver(()=>{this._detachContent()}),this._detachContentMutationObserver.observe(this._pane,{childList:!0}))}_detachContent(){(!this._pane||!this._host||this._pane.children.length===0)&&(this._pane&&this._config.panelClass&&this._toggleClasses(this._pane,this._config.panelClass,!1),this._host&&this._host.parentElement&&(this._previousHostParent=this._host.parentElement,this._host.remove()),this._completeDetachContent())}_completeDetachContent(){this._detachContentAfterRenderRef?.destroy(),this._detachContentAfterRenderRef=void 0,this._detachContentMutationObserver?.disconnect()}_disposeScrollStrategy(){let n=this._scrollStrategy;n?.disable(),n?.detach?.()}},zS="cdk-overlay-connected-position-bounding-box",HL=/([A-Za-z%]+)$/;function Na(t,n){return new Ff(n,t.get(ao),t.get(K),t.get(ye),t.get(jf))}var Ff=class{_viewportRuler;_document;_platform;_overlayContainer;_overlayRef;_isInitialRender=!1;_lastBoundingBoxSize={width:0,height:0};_isPushed=!1;_canPush=!0;_growAfterOpen=!1;_hasFlexibleDimensions=!0;_positionLocked=!1;_originRect;_overlayRect;_viewportRect;_containerRect;_viewportMargin=0;_scrollables=[];_preferredPositions=[];_origin;_pane;_isDisposed=!1;_boundingBox=null;_lastPosition=null;_lastScrollVisibility=null;_positionChanges=new I;_resizeSubscription=ge.EMPTY;_offsetX=0;_offsetY=0;_transformOriginSelector;_appliedPanelClasses=[];_previousPushAmount=null;_popoverLocation="global";positionChanges=this._positionChanges;get positions(){return this._preferredPositions}constructor(n,e,i,r,o){this._viewportRuler=e,this._document=i,this._platform=r,this._overlayContainer=o,this.setOrigin(n)}attach(n){this._overlayRef&&this._overlayRef,this._validatePositions(),n.hostElement.classList.add(zS),this._overlayRef=n,this._boundingBox=n.hostElement,this._pane=n.overlayElement,this._isDisposed=!1,this._isInitialRender=!0,this._lastPosition=null,this._resizeSubscription.unsubscribe(),this._resizeSubscription=this._viewportRuler.change().subscribe(()=>{this._isInitialRender=!0,this.apply()})}apply(){if(this._isDisposed||!this._platform.isBrowser)return;if(!this._isInitialRender&&this._positionLocked&&this._lastPosition){this.reapplyLastPosition();return}this._clearPanelClasses(),this._resetOverlayElementStyles(),this._resetBoundingBoxStyles(),this._viewportRect=this._getNarrowedViewportRect(),this._originRect=this._getOriginRect(),this._overlayRect=this._pane.getBoundingClientRect(),this._containerRect=this._getContainerRect();let n=this._originRect,e=this._overlayRect,i=this._viewportRect,r=this._containerRect,o=[],s;for(let a of this._preferredPositions){let c=this._getOriginPoint(n,r,a),l=this._getOverlayPoint(c,e,a),u=this._getOverlayFit(l,e,i,a);if(u.isCompletelyWithinViewport){this._isPushed=!1,this._applyPosition(a,c);return}if(this._canFitWithFlexibleDimensions(u,l,i)){o.push({position:a,origin:c,overlayRect:e,boundingBoxRect:this._calculateBoundingBoxRect(c,a)});continue}(!s||s.overlayFit.visibleArea<u.visibleArea)&&(s={overlayFit:u,overlayPoint:l,originPoint:c,position:a,overlayRect:e})}if(o.length){let a=null,c=-1;for(let l of o){let u=l.boundingBoxRect.width*l.boundingBoxRect.height*(l.position.weight||1);u>c&&(c=u,a=l)}this._isPushed=!1,this._applyPosition(a.position,a.origin);return}if(this._canPush){this._isPushed=!0,this._applyPosition(s.position,s.originPoint);return}this._applyPosition(s.position,s.originPoint)}detach(){this._clearPanelClasses(),this._lastPosition=null,this._previousPushAmount=null,this._resizeSubscription.unsubscribe()}dispose(){this._isDisposed||(this._boundingBox&&vs(this._boundingBox.style,{top:"",left:"",right:"",bottom:"",height:"",width:"",alignItems:"",justifyContent:""}),this._pane&&this._resetOverlayElementStyles(),this._overlayRef&&this._overlayRef.hostElement.classList.remove(zS),this.detach(),this._positionChanges.complete(),this._overlayRef=this._boundingBox=null,this._isDisposed=!0)}reapplyLastPosition(){if(this._isDisposed||!this._platform.isBrowser)return;let n=this._lastPosition;n?(this._originRect=this._getOriginRect(),this._overlayRect=this._pane.getBoundingClientRect(),this._viewportRect=this._getNarrowedViewportRect(),this._containerRect=this._getContainerRect(),this._applyPosition(n,this._getOriginPoint(this._originRect,this._containerRect,n))):this.apply()}withScrollableContainers(n){return this._scrollables=n,this}withPositions(n){return this._preferredPositions=n,n.indexOf(this._lastPosition)===-1&&(this._lastPosition=null),this._validatePositions(),this}withViewportMargin(n){return this._viewportMargin=n,this}withFlexibleDimensions(n=!0){return this._hasFlexibleDimensions=n,this}withGrowAfterOpen(n=!0){return this._growAfterOpen=n,this}withPush(n=!0){return this._canPush=n,this}withLockedPosition(n=!0){return this._positionLocked=n,this}setOrigin(n){return this._origin=n,this}withDefaultOffsetX(n){return this._offsetX=n,this}withDefaultOffsetY(n){return this._offsetY=n,this}withTransformOriginOn(n){return this._transformOriginSelector=n,this}withPopoverLocation(n){return this._popoverLocation=n,this}getPopoverInsertionPoint(){return this._popoverLocation==="global"?null:this._popoverLocation!=="inline"?this._popoverLocation:this._origin instanceof L?this._origin.nativeElement:Nb(this._origin)?this._origin:null}_getOriginPoint(n,e,i){let r;if(i.originX=="center")r=n.left+n.width/2;else{let s=this._isRtl()?n.right:n.left,a=this._isRtl()?n.left:n.right;r=i.originX=="start"?s:a}e.left<0&&(r-=e.left);let o;return i.originY=="center"?o=n.top+n.height/2:o=i.originY=="top"?n.top:n.bottom,e.top<0&&(o-=e.top),{x:r,y:o}}_getOverlayPoint(n,e,i){let r;i.overlayX=="center"?r=-e.width/2:i.overlayX==="start"?r=this._isRtl()?-e.width:0:r=this._isRtl()?0:-e.width;let o;return i.overlayY=="center"?o=-e.height/2:o=i.overlayY=="top"?0:-e.height,{x:n.x+r,y:n.y+o}}_getOverlayFit(n,e,i,r){let o=GS(e),{x:s,y:a}=n,c=this._getOffset(r,"x"),l=this._getOffset(r,"y");c&&(s+=c),l&&(a+=l);let u=0-s,m=s+o.width-i.width,g=0-a,v=a+o.height-i.height,x=this._subtractOverflows(o.width,u,m),F=this._subtractOverflows(o.height,g,v),j=x*F;return{visibleArea:j,isCompletelyWithinViewport:o.width*o.height===j,fitsInViewportVertically:F===o.height,fitsInViewportHorizontally:x==o.width}}_canFitWithFlexibleDimensions(n,e,i){if(this._hasFlexibleDimensions){let r=i.bottom-e.y,o=i.right-e.x,s=$S(this._overlayRef.getConfig().minHeight),a=$S(this._overlayRef.getConfig().minWidth),c=n.fitsInViewportVertically||s!=null&&s<=r,l=n.fitsInViewportHorizontally||a!=null&&a<=o;return c&&l}return!1}_pushOverlayOnScreen(n,e,i){if(this._previousPushAmount&&this._positionLocked)return{x:n.x+this._previousPushAmount.x,y:n.y+this._previousPushAmount.y};let r=GS(e),o=this._viewportRect,s=Math.max(n.x+r.width-o.width,0),a=Math.max(n.y+r.height-o.height,0),c=Math.max(o.top-i.top-n.y,0),l=Math.max(o.left-i.left-n.x,0),u=0,m=0;return r.width<=o.width?u=l||-s:u=n.x<this._getViewportMarginStart()?o.left-i.left-n.x:0,r.height<=o.height?m=c||-a:m=n.y<this._getViewportMarginTop()?o.top-i.top-n.y:0,this._previousPushAmount={x:u,y:m},{x:n.x+u,y:n.y+m}}_applyPosition(n,e){if(this._setTransformOrigin(n),this._setOverlayElementStyles(e,n),this._setBoundingBoxStyles(e,n),n.panelClass&&this._addPanelClasses(n.panelClass),this._positionChanges.observers.length){let i=this._getScrollVisibility();if(n!==this._lastPosition||!this._lastScrollVisibility||!UL(this._lastScrollVisibility,i)){let r=new Pf(n,i);this._positionChanges.next(r)}this._lastScrollVisibility=i}this._lastPosition=n,this._isInitialRender=!1}_setTransformOrigin(n){if(!this._transformOriginSelector)return;let e=this._boundingBox.querySelectorAll(this._transformOriginSelector),i,r=n.overlayY;n.overlayX==="center"?i="center":this._isRtl()?i=n.overlayX==="start"?"right":"left":i=n.overlayX==="start"?"left":"right";for(let o=0;o<e.length;o++)e[o].style.transformOrigin=`${i} ${r}`}_calculateBoundingBoxRect(n,e){let i=this._viewportRect,r=this._isRtl(),o,s,a;if(e.overlayY==="top")s=n.y,o=i.height-s+this._getViewportMarginBottom();else if(e.overlayY==="bottom")a=i.height-n.y+this._getViewportMarginTop()+this._getViewportMarginBottom(),o=i.height-a+this._getViewportMarginTop();else{let v=Math.min(i.bottom-n.y+i.top,n.y),x=this._lastBoundingBoxSize.height;o=v*2,s=n.y-v,o>x&&!this._isInitialRender&&!this._growAfterOpen&&(s=n.y-x/2)}let c=e.overlayX==="start"&&!r||e.overlayX==="end"&&r,l=e.overlayX==="end"&&!r||e.overlayX==="start"&&r,u,m,g;if(l)g=i.width-n.x+this._getViewportMarginStart()+this._getViewportMarginEnd(),u=n.x-this._getViewportMarginStart();else if(c)m=n.x,u=i.right-n.x-this._getViewportMarginEnd();else{let v=Math.min(i.right-n.x+i.left,n.x),x=this._lastBoundingBoxSize.width;u=v*2,m=n.x-v,u>x&&!this._isInitialRender&&!this._growAfterOpen&&(m=n.x-x/2)}return{top:s,left:m,bottom:a,right:g,width:u,height:o}}_setBoundingBoxStyles(n,e){let i=this._calculateBoundingBoxRect(n,e);!this._isInitialRender&&!this._growAfterOpen&&(i.height=Math.min(i.height,this._lastBoundingBoxSize.height),i.width=Math.min(i.width,this._lastBoundingBoxSize.width));let r={};if(this._hasExactPosition())r.top=r.left="0",r.bottom=r.right="auto",r.maxHeight=r.maxWidth="",r.width=r.height="100%";else{let o=this._overlayRef.getConfig().maxHeight,s=this._overlayRef.getConfig().maxWidth;r.width=At(i.width),r.height=At(i.height),r.top=At(i.top)||"auto",r.bottom=At(i.bottom)||"auto",r.left=At(i.left)||"auto",r.right=At(i.right)||"auto",e.overlayX==="center"?r.alignItems="center":r.alignItems=e.overlayX==="end"?"flex-end":"flex-start",e.overlayY==="center"?r.justifyContent="center":r.justifyContent=e.overlayY==="bottom"?"flex-end":"flex-start",o&&(r.maxHeight=At(o)),s&&(r.maxWidth=At(s))}this._lastBoundingBoxSize=i,vs(this._boundingBox.style,r)}_resetBoundingBoxStyles(){vs(this._boundingBox.style,{top:"0",left:"0",right:"0",bottom:"0",height:"",width:"",alignItems:"",justifyContent:""})}_resetOverlayElementStyles(){vs(this._pane.style,{top:"",left:"",bottom:"",right:"",position:"",transform:""})}_setOverlayElementStyles(n,e){let i={},r=this._hasExactPosition(),o=this._hasFlexibleDimensions,s=this._overlayRef.getConfig();if(r){let u=this._viewportRuler.getViewportScrollPosition();vs(i,this._getExactOverlayY(e,n,u)),vs(i,this._getExactOverlayX(e,n,u))}else i.position="static";let a="",c=this._getOffset(e,"x"),l=this._getOffset(e,"y");c&&(a+=`translateX(${c}px) `),l&&(a+=`translateY(${l}px)`),i.transform=a.trim(),s.maxHeight&&(r?i.maxHeight=At(s.maxHeight):o&&(i.maxHeight="")),s.maxWidth&&(r?i.maxWidth=At(s.maxWidth):o&&(i.maxWidth="")),vs(this._pane.style,i)}_getExactOverlayY(n,e,i){let r={top:"",bottom:""},o=this._getOverlayPoint(e,this._overlayRect,n);if(this._isPushed&&(o=this._pushOverlayOnScreen(o,this._overlayRect,i)),n.overlayY==="bottom"){let s=this._document.documentElement.clientHeight;r.bottom=`${s-(o.y+this._overlayRect.height)}px`}else r.top=At(o.y);return r}_getExactOverlayX(n,e,i){let r={left:"",right:""},o=this._getOverlayPoint(e,this._overlayRect,n);this._isPushed&&(o=this._pushOverlayOnScreen(o,this._overlayRect,i));let s;if(this._isRtl()?s=n.overlayX==="end"?"left":"right":s=n.overlayX==="end"?"right":"left",s==="right"){let a=this._document.documentElement.clientWidth;r.right=`${a-(o.x+this._overlayRect.width)}px`}else r.left=At(o.x);return r}_getScrollVisibility(){let n=this._getOriginRect(),e=this._pane.getBoundingClientRect(),i=this._scrollables.map(r=>r.getElementRef().nativeElement.getBoundingClientRect());return{isOriginClipped:HS(n,i),isOriginOutsideView:Rb(n,i),isOverlayClipped:HS(e,i),isOverlayOutsideView:Rb(e,i)}}_subtractOverflows(n,...e){return e.reduce((i,r)=>i-Math.max(r,0),n)}_getNarrowedViewportRect(){let n=this._document.documentElement.clientWidth,e=this._document.documentElement.clientHeight,i=this._viewportRuler.getViewportScrollPosition();return{top:i.top+this._getViewportMarginTop(),left:i.left+this._getViewportMarginStart(),right:i.left+n-this._getViewportMarginEnd(),bottom:i.top+e-this._getViewportMarginBottom(),width:n-this._getViewportMarginStart()-this._getViewportMarginEnd(),height:e-this._getViewportMarginTop()-this._getViewportMarginBottom()}}_isRtl(){return this._overlayRef.getDirection()==="rtl"}_hasExactPosition(){return!this._hasFlexibleDimensions||this._isPushed}_getOffset(n,e){return e==="x"?n.offsetX==null?this._offsetX:n.offsetX:n.offsetY==null?this._offsetY:n.offsetY}_validatePositions(){}_addPanelClasses(n){this._pane&&Ea(n).forEach(e=>{e!==""&&this._appliedPanelClasses.indexOf(e)===-1&&(this._appliedPanelClasses.push(e),this._pane.classList.add(e))})}_clearPanelClasses(){this._pane&&(this._appliedPanelClasses.forEach(n=>{this._pane.classList.remove(n)}),this._appliedPanelClasses=[])}_getViewportMarginStart(){return typeof this._viewportMargin=="number"?this._viewportMargin:this._viewportMargin?.start??0}_getViewportMarginEnd(){return typeof this._viewportMargin=="number"?this._viewportMargin:this._viewportMargin?.end??0}_getViewportMarginTop(){return typeof this._viewportMargin=="number"?this._viewportMargin:this._viewportMargin?.top??0}_getViewportMarginBottom(){return typeof this._viewportMargin=="number"?this._viewportMargin:this._viewportMargin?.bottom??0}_getOriginRect(){let n=this._origin;if(n instanceof L)return n.nativeElement.getBoundingClientRect();if(n instanceof Element)return n.getBoundingClientRect();let e=n.width||0,i=n.height||0;return{top:n.y,bottom:n.y+i,left:n.x,right:n.x+e,height:i,width:e}}_getContainerRect(){let n=this._overlayRef.getConfig().usePopover&&this._popoverLocation!=="global",e=this._overlayContainer.getContainerElement();n&&(e.style.display="block");let i=e.getBoundingClientRect();return n&&(e.style.display=""),i}};function vs(t,n){for(let e in n)n.hasOwnProperty(e)&&(t[e]=n[e]);return t}function $S(t){if(typeof t!="number"&&t!=null){let[n,e]=t.split(HL);return!e||e==="px"?parseFloat(n):null}return t||null}function GS(t){return{top:Math.floor(t.top),right:Math.floor(t.right),bottom:Math.floor(t.bottom),left:Math.floor(t.left),width:Math.floor(t.width),height:Math.floor(t.height)}}function UL(t,n){return t===n?!0:t.isOriginClipped===n.isOriginClipped&&t.isOriginOutsideView===n.isOriginOutsideView&&t.isOverlayClipped===n.isOverlayClipped&&t.isOverlayOutsideView===n.isOverlayOutsideView}var WS="cdk-global-overlay-wrapper";function bs(t){return new Lf}var Lf=class{_overlayRef;_cssPosition="static";_topOffset="";_bottomOffset="";_alignItems="";_xPosition="";_xOffset="";_width="";_height="";_isDisposed=!1;attach(n){let e=n.getConfig();this._overlayRef=n,this._width&&!e.width&&n.updateSize({width:this._width}),this._height&&!e.height&&n.updateSize({height:this._height}),n.hostElement.classList.add(WS),this._isDisposed=!1}top(n=""){return this._bottomOffset="",this._topOffset=n,this._alignItems="flex-start",this}left(n=""){return this._xOffset=n,this._xPosition="left",this}bottom(n=""){return this._topOffset="",this._bottomOffset=n,this._alignItems="flex-end",this}right(n=""){return this._xOffset=n,this._xPosition="right",this}start(n=""){return this._xOffset=n,this._xPosition="start",this}end(n=""){return this._xOffset=n,this._xPosition="end",this}width(n=""){return this._overlayRef?this._overlayRef.updateSize({width:n}):this._width=n,this}height(n=""){return this._overlayRef?this._overlayRef.updateSize({height:n}):this._height=n,this}centerHorizontally(n=""){return this.left(n),this._xPosition="center",this}centerVertically(n=""){return this.top(n),this._alignItems="center",this}apply(){if(!this._overlayRef||!this._overlayRef.hasAttached())return;let n=this._overlayRef.overlayElement.style,e=this._overlayRef.hostElement.style,i=this._overlayRef.getConfig(),{width:r,height:o,maxWidth:s,maxHeight:a}=i,c=(r==="100%"||r==="100vw")&&(!s||s==="100%"||s==="100vw"),l=(o==="100%"||o==="100vh")&&(!a||a==="100%"||a==="100vh"),u=this._xPosition,m=this._xOffset,g=this._overlayRef.getConfig().direction==="rtl",v="",x="",F="";c?F="flex-start":u==="center"?(F="center",g?x=m:v=m):g?u==="left"||u==="end"?(F="flex-end",v=m):(u==="right"||u==="start")&&(F="flex-start",x=m):u==="left"||u==="start"?(F="flex-start",v=m):(u==="right"||u==="end")&&(F="flex-end",x=m),n.position=this._cssPosition,n.marginLeft=c?"0":v,n.marginTop=l?"0":this._topOffset,n.marginBottom=this._bottomOffset,n.marginRight=c?"0":x,e.justifyContent=F,e.alignItems=l?"flex-start":this._alignItems}dispose(){if(this._isDisposed||!this._overlayRef)return;let n=this._overlayRef.overlayElement.style,e=this._overlayRef.hostElement,i=e.style;e.classList.remove(WS),i.justifyContent=i.alignItems=n.marginTop=n.marginBottom=n.marginLeft=n.marginRight=n.position="",this._overlayRef=null,this._isDisposed=!0}};var Yl=new y("OVERLAY_DEFAULT_CONFIG");function Ji(t,n){t.get(qe).load(QS);let e=t.get(jf),i=t.get(K),r=t.get(Ie),o=t.get(Sn),s=t.get(Xt),a=t.get(xe,null,{optional:!0})||t.get(Ot).createRenderer(null,null),c=new wi(n),l=t.get(Yl,null,{optional:!0})?.usePopover??!0;c.direction=c.direction||s.value,"showPopover"in i.body?c.usePopover=n?.usePopover??l:c.usePopover=!1;let u=i.createElement("div"),m=i.createElement("div");u.id=r.getId("cdk-overlay-"),u.classList.add("cdk-overlay-pane"),m.appendChild(u),c.usePopover&&(m.setAttribute("popover","manual"),m.classList.add("cdk-overlay-popover"));let g=c.usePopover?c.positionStrategy?.getPopoverInsertionPoint?.():null;return Nb(g)?g.after(m):g?.type==="parent"?g.element.appendChild(m):e.getContainerElement().appendChild(m),new Ra(new Wl(u,o,t),m,u,c,t.get(H),t.get(YS),i,t.get(Gi),t.get(ZS),n?.disableAnimations??t.get(Gc,null,{optional:!0})==="NoopAnimations",t.get(He),a)}var zL=[{originX:"start",originY:"bottom",overlayX:"start",overlayY:"top"},{originX:"start",originY:"top",overlayX:"start",overlayY:"bottom"},{originX:"end",originY:"top",overlayX:"end",overlayY:"bottom"},{originX:"end",originY:"bottom",overlayX:"end",overlayY:"top"}],$L=new y("cdk-connected-overlay-scroll-strategy",{providedIn:"root",factory:()=>{let t=d(X);return()=>lo(t)}}),Oa=(()=>{class t{elementRef=d(L);constructor(){}static \u0275fac=function(i){return new(i||t)};static \u0275dir=A({type:t,selectors:[["","cdk-overlay-origin",""],["","overlay-origin",""],["","cdkOverlayOrigin",""]],exportAs:["cdkOverlayOrigin"]})}return t})(),XS=new y("cdk-connected-overlay-default-config"),Bf=(()=>{class t{_dir=d(Xt,{optional:!0});_injector=d(X);_overlayRef;_templatePortal;_backdropSubscription=ge.EMPTY;_attachSubscription=ge.EMPTY;_detachSubscription=ge.EMPTY;_positionSubscription=ge.EMPTY;_offsetX;_offsetY;_position;_scrollStrategyFactory=d($L);_ngZone=d(H);origin;positions;positionStrategy;get offsetX(){return this._offsetX}set offsetX(e){this._offsetX=e,this._position&&this._updatePositionStrategy(this._position)}get offsetY(){return this._offsetY}set offsetY(e){this._offsetY=e,this._position&&this._updatePositionStrategy(this._position)}width;height;minWidth;minHeight;backdropClass;panelClass;viewportMargin=0;scrollStrategy;open=!1;disableClose=!1;transformOriginSelector;hasBackdrop=!1;lockPosition=!1;flexibleDimensions=!1;growAfterOpen=!1;push=!1;disposeOnNavigation=!1;usePopover;matchWidth=!1;set _config(e){typeof e!="string"&&this._assignConfig(e)}backdropClick=new U;positionChange=new U;attach=new U;detach=new U;overlayKeydown=new U;overlayOutsideClick=new U;constructor(){let e=d(vt),i=d(Vt),r=d(XS,{optional:!0}),o=d(Yl,{optional:!0});this.usePopover=o?.usePopover===!1?null:"global",this._templatePortal=new Un(e,i),this.scrollStrategy=this._scrollStrategyFactory(),r&&this._assignConfig(r)}get overlayRef(){return this._overlayRef}get dir(){return this._dir?this._dir.value:"ltr"}ngOnDestroy(){this._attachSubscription.unsubscribe(),this._detachSubscription.unsubscribe(),this._backdropSubscription.unsubscribe(),this._positionSubscription.unsubscribe(),this._overlayRef?.dispose()}ngOnChanges(e){this._position&&(this._updatePositionStrategy(this._position),this._overlayRef?.updateSize({width:this._getWidth(),minWidth:this.minWidth,height:this.height,minHeight:this.minHeight}),e.origin&&this.open&&this._position.apply()),e.open&&(this.open?this.attachOverlay():this.detachOverlay())}_createOverlay(){(!this.positions||!this.positions.length)&&(this.positions=zL);let e=this._overlayRef=Ji(this._injector,this._buildConfig());this._attachSubscription=e.attachments().subscribe(()=>this.attach.emit()),this._detachSubscription=e.detachments().subscribe(()=>this.detach.emit()),e.keydownEvents().subscribe(i=>{this.overlayKeydown.next(i),i.keyCode===27&&!this.disableClose&&!Ct(i)&&(i.preventDefault(),this.detachOverlay())}),this._overlayRef.outsidePointerEvents().subscribe(i=>{let r=this._getOriginElement(),o=Kt(i);(!r||r!==o&&!r.contains(o))&&this.overlayOutsideClick.next(i)})}_buildConfig(){let e=this._position=this.positionStrategy||this._createPositionStrategy(),i=new wi({direction:this._dir||"ltr",positionStrategy:e,scrollStrategy:this.scrollStrategy,hasBackdrop:this.hasBackdrop,disposeOnNavigation:this.disposeOnNavigation,usePopover:!!this.usePopover});return(this.height||this.height===0)&&(i.height=this.height),(this.minWidth||this.minWidth===0)&&(i.minWidth=this.minWidth),(this.minHeight||this.minHeight===0)&&(i.minHeight=this.minHeight),this.backdropClass&&(i.backdropClass=this.backdropClass),this.panelClass&&(i.panelClass=this.panelClass),i}_updatePositionStrategy(e){let i=this.positions.map(r=>({originX:r.originX,originY:r.originY,overlayX:r.overlayX,overlayY:r.overlayY,offsetX:r.offsetX||this.offsetX,offsetY:r.offsetY||this.offsetY,panelClass:r.panelClass||void 0}));return e.setOrigin(this._getOrigin()).withPositions(i).withFlexibleDimensions(this.flexibleDimensions).withPush(this.push).withGrowAfterOpen(this.growAfterOpen).withViewportMargin(this.viewportMargin).withLockedPosition(this.lockPosition).withTransformOriginOn(this.transformOriginSelector).withPopoverLocation(this.usePopover===null?"global":this.usePopover)}_createPositionStrategy(){let e=Na(this._injector,this._getOrigin());return this._updatePositionStrategy(e),e}_getOrigin(){return this.origin instanceof Oa?this.origin.elementRef:this.origin}_getOriginElement(){return this.origin instanceof Oa?this.origin.elementRef.nativeElement:this.origin instanceof L?this.origin.nativeElement:typeof Element<"u"&&this.origin instanceof Element?this.origin:null}_getWidth(){return this.width?this.width:this.matchWidth?this._getOriginElement()?.getBoundingClientRect?.().width:void 0}attachOverlay(){this._overlayRef||this._createOverlay();let e=this._overlayRef;e.getConfig().hasBackdrop=this.hasBackdrop,e.updateSize({width:this._getWidth()}),e.hasAttached()||e.attach(this._templatePortal),this.hasBackdrop?this._backdropSubscription=e.backdropClick().subscribe(i=>this.backdropClick.emit(i)):this._backdropSubscription.unsubscribe(),this._positionSubscription.unsubscribe(),this.positionChange.observers.length>0&&(this._positionSubscription=this._position.positionChanges.pipe(xp(()=>this.positionChange.observers.length>0)).subscribe(i=>{this._ngZone.run(()=>this.positionChange.emit(i)),this.positionChange.observers.length===0&&this._positionSubscription.unsubscribe()})),this.open=!0}detachOverlay(){this._overlayRef?.detach(),this._backdropSubscription.unsubscribe(),this._positionSubscription.unsubscribe(),this.open=!1}_assignConfig(e){this.origin=e.origin??this.origin,this.positions=e.positions??this.positions,this.positionStrategy=e.positionStrategy??this.positionStrategy,this.offsetX=e.offsetX??this.offsetX,this.offsetY=e.offsetY??this.offsetY,this.width=e.width??this.width,this.height=e.height??this.height,this.minWidth=e.minWidth??this.minWidth,this.minHeight=e.minHeight??this.minHeight,this.backdropClass=e.backdropClass??this.backdropClass,this.panelClass=e.panelClass??this.panelClass,this.viewportMargin=e.viewportMargin??this.viewportMargin,this.scrollStrategy=e.scrollStrategy??this.scrollStrategy,this.disableClose=e.disableClose??this.disableClose,this.transformOriginSelector=e.transformOriginSelector??this.transformOriginSelector,this.hasBackdrop=e.hasBackdrop??this.hasBackdrop,this.lockPosition=e.lockPosition??this.lockPosition,this.flexibleDimensions=e.flexibleDimensions??this.flexibleDimensions,this.growAfterOpen=e.growAfterOpen??this.growAfterOpen,this.push=e.push??this.push,this.disposeOnNavigation=e.disposeOnNavigation??this.disposeOnNavigation,this.usePopover=e.usePopover??this.usePopover,this.matchWidth=e.matchWidth??this.matchWidth}static \u0275fac=function(i){return new(i||t)};static \u0275dir=A({type:t,selectors:[["","cdk-connected-overlay",""],["","connected-overlay",""],["","cdkConnectedOverlay",""]],inputs:{origin:[0,"cdkConnectedOverlayOrigin","origin"],positions:[0,"cdkConnectedOverlayPositions","positions"],positionStrategy:[0,"cdkConnectedOverlayPositionStrategy","positionStrategy"],offsetX:[0,"cdkConnectedOverlayOffsetX","offsetX"],offsetY:[0,"cdkConnectedOverlayOffsetY","offsetY"],width:[0,"cdkConnectedOverlayWidth","width"],height:[0,"cdkConnectedOverlayHeight","height"],minWidth:[0,"cdkConnectedOverlayMinWidth","minWidth"],minHeight:[0,"cdkConnectedOverlayMinHeight","minHeight"],backdropClass:[0,"cdkConnectedOverlayBackdropClass","backdropClass"],panelClass:[0,"cdkConnectedOverlayPanelClass","panelClass"],viewportMargin:[0,"cdkConnectedOverlayViewportMargin","viewportMargin"],scrollStrategy:[0,"cdkConnectedOverlayScrollStrategy","scrollStrategy"],open:[0,"cdkConnectedOverlayOpen","open"],disableClose:[0,"cdkConnectedOverlayDisableClose","disableClose"],transformOriginSelector:[0,"cdkConnectedOverlayTransformOriginOn","transformOriginSelector"],hasBackdrop:[2,"cdkConnectedOverlayHasBackdrop","hasBackdrop",B],lockPosition:[2,"cdkConnectedOverlayLockPosition","lockPosition",B],flexibleDimensions:[2,"cdkConnectedOverlayFlexibleDimensions","flexibleDimensions",B],growAfterOpen:[2,"cdkConnectedOverlayGrowAfterOpen","growAfterOpen",B],push:[2,"cdkConnectedOverlayPush","push",B],disposeOnNavigation:[2,"cdkConnectedOverlayDisposeOnNavigation","disposeOnNavigation",B],usePopover:[0,"cdkConnectedOverlayUsePopover","usePopover"],matchWidth:[2,"cdkConnectedOverlayMatchWidth","matchWidth",B],_config:[0,"cdkConnectedOverlay","_config"]},outputs:{backdropClick:"backdropClick",positionChange:"positionChange",attach:"attach",detach:"detach",overlayKeydown:"overlayKeydown",overlayOutsideClick:"overlayOutsideClick"},exportAs:["cdkConnectedOverlay"],features:[ze]})}return t})();function WL(t,n){}var uo=class{viewContainerRef;injector;id;role="dialog";panelClass="";hasBackdrop=!0;backdropClass="";disableClose=!1;closePredicate;width="";height="";minWidth;minHeight;maxWidth;maxHeight;positionStrategy;data=null;direction;ariaDescribedBy=null;ariaLabelledBy=null;ariaLabel=null;ariaModal=!1;autoFocus="first-tabbable";restoreFocus=!0;scrollStrategy;closeOnNavigation=!0;closeOnDestroy=!0;closeOnOverlayDetachments=!0;disableAnimations=!1;providers;container;templateContext};var Fb=(()=>{class t extends co{_elementRef=d(L);_focusTrapFactory=d(bb);_config;_interactivityChecker=d(Ia);_ngZone=d(H);_focusMonitor=d(Tn);_renderer=d(xe);_changeDetectorRef=d(ve);_injector=d(X);_platform=d(ye);_document=d(K);_portalOutlet;_focusTrapped=new I;_focusTrap=null;_elementFocusedBeforeDialogWasOpened=null;_closeInteractionType=null;_ariaLabelledByQueue=[];_isDestroyed=!1;constructor(){super(),this._config=d(uo,{optional:!0})||new uo,this._config.ariaLabelledBy&&this._ariaLabelledByQueue.push(this._config.ariaLabelledBy)}_addAriaLabelledBy(e){this._ariaLabelledByQueue.push(e),this._changeDetectorRef.markForCheck()}_removeAriaLabelledBy(e){let i=this._ariaLabelledByQueue.indexOf(e);i>-1&&(this._ariaLabelledByQueue.splice(i,1),this._changeDetectorRef.markForCheck())}_contentAttached(){this._initializeFocusTrap(),this._captureInitialFocus()}_captureInitialFocus(){this._trapFocus()}ngOnDestroy(){this._focusTrapped.complete(),this._isDestroyed=!0,this._restoreFocus()}attachComponentPortal(e){this._portalOutlet.hasAttached();let i=this._portalOutlet.attachComponentPortal(e);return this._contentAttached(),i}attachTemplatePortal(e){this._portalOutlet.hasAttached();let i=this._portalOutlet.attachTemplatePortal(e);return this._contentAttached(),i}attachDomPortal=e=>{this._portalOutlet.hasAttached();let i=this._portalOutlet.attachDomPortal(e);return this._contentAttached(),i};_recaptureFocus(){this._containsFocus()||this._trapFocus()}_forceFocus(e,i){this._interactivityChecker.isFocusable(e)||(e.tabIndex=-1,this._ngZone.runOutsideAngular(()=>{let r=()=>{o(),s(),e.removeAttribute("tabindex")},o=this._renderer.listen(e,"blur",r),s=this._renderer.listen(e,"mousedown",r)})),e.focus(i)}_focusByCssSelector(e,i){let r=this._elementRef.nativeElement.querySelector(e);r&&this._forceFocus(r,i)}_trapFocus(e){this._isDestroyed||bt(()=>{let i=this._elementRef.nativeElement;switch(this._config.autoFocus){case!1:case"dialog":this._containsFocus()||i.focus(e);break;case!0:case"first-tabbable":this._focusTrap?.focusInitialElement(e)||this._focusDialogContainer(e);break;case"first-heading":this._focusByCssSelector('h1, h2, h3, h4, h5, h6, [role="heading"]',e);break;default:this._focusByCssSelector(this._config.autoFocus,e);break}this._focusTrapped.next()},{injector:this._injector})}_restoreFocus(){let e=this._config.restoreFocus,i=null;if(typeof e=="string"?i=this._document.querySelector(e):typeof e=="boolean"?i=e?this._elementFocusedBeforeDialogWasOpened:null:e&&(i=e),this._config.restoreFocus&&i&&typeof i.focus=="function"){let r=yr(),o=this._elementRef.nativeElement;(!r||r===this._document.body||r===o||o.contains(r))&&(this._focusMonitor?(this._focusMonitor.focusVia(i,this._closeInteractionType),this._closeInteractionType=null):i.focus())}this._focusTrap&&this._focusTrap.destroy()}_focusDialogContainer(e){this._elementRef.nativeElement.focus?.(e)}_containsFocus(){let e=this._elementRef.nativeElement,i=yr();return e===i||e.contains(i)}_initializeFocusTrap(){this._platform.isBrowser&&(this._focusTrap=this._focusTrapFactory.create(this._elementRef.nativeElement),this._document&&(this._elementFocusedBeforeDialogWasOpened=yr()))}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=E({type:t,selectors:[["cdk-dialog-container"]],viewQuery:function(i,r){if(i&1&&Ve(Ki,7),i&2){let o;$(o=G())&&(r._portalOutlet=o.first)}},hostAttrs:["tabindex","-1",1,"cdk-dialog-container"],hostVars:6,hostBindings:function(i,r){i&2&&q("id",r._config.id||null)("role",r._config.role)("aria-modal",r._config.ariaModal)("aria-labelledby",r._config.ariaLabel?null:r._ariaLabelledByQueue[0])("aria-label",r._config.ariaLabel)("aria-describedby",r._config.ariaDescribedBy||null)},features:[fe],decls:1,vars:0,consts:[["cdkPortalOutlet",""]],template:function(i,r){i&1&&$e(0,WL,0,0,"ng-template",0)},dependencies:[Ki],styles:[`.cdk-dialog-container {
  display: block;
  width: 100%;
  height: 100%;
  min-height: inherit;
  max-height: inherit;
}
`],encapsulation:2})}return t})(),Ql=class{overlayRef;config;componentInstance=null;componentRef=null;containerInstance;disableClose;closed=new I;backdropClick;keydownEvents;outsidePointerEvents;id;_detachSubscription;constructor(n,e){this.overlayRef=n,this.config=e,this.disableClose=e.disableClose,this.backdropClick=n.backdropClick(),this.keydownEvents=n.keydownEvents(),this.outsidePointerEvents=n.outsidePointerEvents(),this.id=e.id,this.keydownEvents.subscribe(i=>{i.keyCode===27&&!this.disableClose&&!Ct(i)&&(i.preventDefault(),this.close(void 0,{focusOrigin:"keyboard"}))}),this.backdropClick.subscribe(()=>{!this.disableClose&&this._canClose()?this.close(void 0,{focusOrigin:"mouse"}):this.containerInstance._recaptureFocus?.()}),this._detachSubscription=n.detachments().subscribe(()=>{e.closeOnOverlayDetachments!==!1&&this.close()})}close(n,e){if(this._canClose(n)){let i=this.closed;this.containerInstance._closeInteractionType=e?.focusOrigin||"program",this._detachSubscription.unsubscribe(),this.overlayRef.dispose(),i.next(n),i.complete(),this.componentInstance=this.containerInstance=null}}updatePosition(){return this.overlayRef.updatePosition(),this}updateSize(n="",e=""){return this.overlayRef.updateSize({width:n,height:e}),this}addPanelClass(n){return this.overlayRef.addPanelClass(n),this}removePanelClass(n){return this.overlayRef.removePanelClass(n),this}_canClose(n){let e=this.config;return!!this.containerInstance&&(!e.closePredicate||e.closePredicate(n,e,this.componentInstance))}},qL=new y("DialogScrollStrategy",{providedIn:"root",factory:()=>{let t=d(X);return()=>ql(t)}}),YL=new y("DialogData"),ZL=new y("DefaultDialogConfig");function QL(t){let n=M(t),e=new U;return{valueSignal:n,get value(){return n()},change:e,ngOnDestroy(){e.complete()}}}var KS=(()=>{class t{_injector=d(X);_defaultOptions=d(ZL,{optional:!0});_parentDialog=d(t,{optional:!0,skipSelf:!0});_overlayContainer=d(jf);_idGenerator=d(Ie);_openDialogsAtThisLevel=[];_afterAllClosedAtThisLevel=new I;_afterOpenedAtThisLevel=new I;_ariaHiddenElements=new Map;_scrollStrategy=d(qL);get openDialogs(){return this._parentDialog?this._parentDialog.openDialogs:this._openDialogsAtThisLevel}get afterOpened(){return this._parentDialog?this._parentDialog.afterOpened:this._afterOpenedAtThisLevel}afterAllClosed=ci(()=>this.openDialogs.length?this._getAfterAllClosed():this._getAfterAllClosed().pipe(et(void 0)));constructor(){}open(e,i){let r=this._defaultOptions||new uo;i=b(b({},r),i),i.id=i.id||this._idGenerator.getId("cdk-dialog-"),i.id&&this.getDialogById(i.id);let o=this._getOverlayConfig(i),s=Ji(this._injector,o),a=new Ql(s,i),c=this._attachContainer(s,a,i);if(a.containerInstance=c,!this.openDialogs.length){let l=this._overlayContainer.getContainerElement();c._focusTrapped?c._focusTrapped.pipe(ut(1)).subscribe(()=>{this._hideNonDialogContentFromAssistiveTechnology(l)}):this._hideNonDialogContentFromAssistiveTechnology(l)}return this._attachDialogContent(e,a,c,i),this.openDialogs.push(a),a.closed.subscribe(()=>this._removeOpenDialog(a,!0)),this.afterOpened.next(a),a}closeAll(){Pb(this.openDialogs,e=>e.close())}getDialogById(e){return this.openDialogs.find(i=>i.id===e)}ngOnDestroy(){Pb(this._openDialogsAtThisLevel,e=>{e.config.closeOnDestroy===!1&&this._removeOpenDialog(e,!1)}),Pb(this._openDialogsAtThisLevel,e=>e.close()),this._afterAllClosedAtThisLevel.complete(),this._afterOpenedAtThisLevel.complete(),this._openDialogsAtThisLevel=[]}_getOverlayConfig(e){let i=new wi({positionStrategy:e.positionStrategy||bs().centerHorizontally().centerVertically(),scrollStrategy:e.scrollStrategy||this._scrollStrategy(),panelClass:e.panelClass,hasBackdrop:e.hasBackdrop,direction:e.direction,minWidth:e.minWidth,minHeight:e.minHeight,maxWidth:e.maxWidth,maxHeight:e.maxHeight,width:e.width,height:e.height,disposeOnNavigation:e.closeOnNavigation,disableAnimations:e.disableAnimations});return e.backdropClass&&(i.backdropClass=e.backdropClass),i}_attachContainer(e,i,r){let o=r.injector||r.viewContainerRef?.injector,s=[{provide:uo,useValue:r},{provide:Ql,useValue:i},{provide:Ra,useValue:e}],a;r.container?typeof r.container=="function"?a=r.container:(a=r.container.type,s.push(...r.container.providers(r))):a=Fb;let c=new Ci(a,r.viewContainerRef,X.create({parent:o||this._injector,providers:s}));return e.attach(c).instance}_attachDialogContent(e,i,r,o){if(e instanceof vt){let s=this._createInjector(o,i,r,void 0),a={$implicit:o.data,dialogRef:i};o.templateContext&&(a=b(b({},a),typeof o.templateContext=="function"?o.templateContext():o.templateContext)),r.attachTemplatePortal(new Un(e,null,a,s))}else{let s=this._createInjector(o,i,r,this._injector),a=r.attachComponentPortal(new Ci(e,o.viewContainerRef,s));i.componentRef=a,i.componentInstance=a.instance}}_createInjector(e,i,r,o){let s=e.injector||e.viewContainerRef?.injector,a=[{provide:YL,useValue:e.data},{provide:Ql,useValue:i}];return e.providers&&(typeof e.providers=="function"?a.push(...e.providers(i,e,r)):a.push(...e.providers)),e.direction&&(!s||!s.get(Xt,null,{optional:!0}))&&a.push({provide:Xt,useValue:QL(e.direction)}),X.create({parent:s||o,providers:a})}_removeOpenDialog(e,i){let r=this.openDialogs.indexOf(e);r>-1&&(this.openDialogs.splice(r,1),this.openDialogs.length||(this._ariaHiddenElements.forEach((o,s)=>{o?s.setAttribute("aria-hidden",o):s.removeAttribute("aria-hidden")}),this._ariaHiddenElements.clear(),i&&this._getAfterAllClosed().next()))}_hideNonDialogContentFromAssistiveTechnology(e){if(e.parentElement){let i=e.parentElement.children;for(let r=i.length-1;r>-1;r--){let o=i[r];o!==e&&o.nodeName!=="SCRIPT"&&o.nodeName!=="STYLE"&&!o.hasAttribute("aria-live")&&!o.hasAttribute("popover")&&(this._ariaHiddenElements.set(o,o.getAttribute("aria-hidden")),o.setAttribute("aria-hidden","true"))}}}_getAfterAllClosed(){let e=this._parentDialog;return e?e._getAfterAllClosed():this._afterAllClosedAtThisLevel}static \u0275fac=function(i){return new(i||t)};static \u0275prov=C({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();function Pb(t,n){let e=t.length;for(;e--;)n(t[e])}function XL(t,n){}var Hf=class{viewContainerRef;injector;id;role="dialog";panelClass="";hasBackdrop=!0;backdropClass="";disableClose=!1;closePredicate;width="";height="";minWidth;minHeight;maxWidth;maxHeight;position;data=null;direction;ariaDescribedBy=null;ariaLabelledBy=null;ariaLabel=null;ariaModal=!1;autoFocus="first-tabbable";restoreFocus=!0;delayFocusTrap=!0;scrollStrategy;closeOnNavigation=!0;enterAnimationDuration;exitAnimationDuration},Lb="mdc-dialog--open",JS="mdc-dialog--opening",eI="mdc-dialog--closing",KL=150,JL=75,e2=(()=>{class t extends Fb{_animationStateChanged=new U;_animationsEnabled=!ke();_actionSectionCount=0;_hostElement=this._elementRef.nativeElement;_enterAnimationDuration=this._animationsEnabled?nI(this._config.enterAnimationDuration)??KL:0;_exitAnimationDuration=this._animationsEnabled?nI(this._config.exitAnimationDuration)??JL:0;_animationTimer=null;_contentAttached(){super._contentAttached(),this._startOpenAnimation()}_startOpenAnimation(){this._animationStateChanged.emit({state:"opening",totalTime:this._enterAnimationDuration}),this._animationsEnabled?(this._hostElement.style.setProperty(tI,`${this._enterAnimationDuration}ms`),this._requestAnimationFrame(()=>this._hostElement.classList.add(JS,Lb)),this._waitForAnimationToComplete(this._enterAnimationDuration,this._finishDialogOpen)):(this._hostElement.classList.add(Lb),Promise.resolve().then(()=>this._finishDialogOpen()))}_startExitAnimation(){this._animationStateChanged.emit({state:"closing",totalTime:this._exitAnimationDuration}),this._hostElement.classList.remove(Lb),this._animationsEnabled?(this._hostElement.style.setProperty(tI,`${this._exitAnimationDuration}ms`),this._requestAnimationFrame(()=>this._hostElement.classList.add(eI)),this._waitForAnimationToComplete(this._exitAnimationDuration,this._finishDialogClose)):Promise.resolve().then(()=>this._finishDialogClose())}_updateActionSectionCount(e){this._actionSectionCount+=e,this._changeDetectorRef.markForCheck()}_finishDialogOpen=()=>{this._clearAnimationClasses(),this._openAnimationDone(this._enterAnimationDuration)};_finishDialogClose=()=>{this._clearAnimationClasses(),this._animationStateChanged.emit({state:"closed",totalTime:this._exitAnimationDuration})};_clearAnimationClasses(){this._hostElement.classList.remove(JS,eI)}_waitForAnimationToComplete(e,i){this._animationTimer!==null&&clearTimeout(this._animationTimer),this._animationTimer=setTimeout(i,e)}_requestAnimationFrame(e){this._ngZone.runOutsideAngular(()=>{typeof requestAnimationFrame=="function"?requestAnimationFrame(e):e()})}_captureInitialFocus(){this._config.delayFocusTrap||this._trapFocus()}_openAnimationDone(e){this._config.delayFocusTrap&&this._trapFocus(),this._animationStateChanged.next({state:"opened",totalTime:e})}ngOnDestroy(){super.ngOnDestroy(),this._animationTimer!==null&&clearTimeout(this._animationTimer)}attachComponentPortal(e){let i=super.attachComponentPortal(e);return i.location.nativeElement.classList.add("mat-mdc-dialog-component-host"),i}static \u0275fac=(()=>{let e;return function(r){return(e||(e=ct(t)))(r||t)}})();static \u0275cmp=E({type:t,selectors:[["mat-dialog-container"]],hostAttrs:["tabindex","-1",1,"mat-mdc-dialog-container","mdc-dialog"],hostVars:10,hostBindings:function(i,r){i&2&&(mt("id",r._config.id),q("aria-modal",r._config.ariaModal)("role",r._config.role)("aria-labelledby",r._config.ariaLabel?null:r._ariaLabelledByQueue[0])("aria-label",r._config.ariaLabel)("aria-describedby",r._config.ariaDescribedBy||null),z("_mat-animation-noopable",!r._animationsEnabled)("mat-mdc-dialog-container-with-actions",r._actionSectionCount>0))},features:[fe],decls:3,vars:0,consts:[[1,"mat-mdc-dialog-inner-container","mdc-dialog__container"],[1,"mat-mdc-dialog-surface","mdc-dialog__surface"],["cdkPortalOutlet",""]],template:function(i,r){i&1&&(f(0,"div",0)(1,"div",1),$e(2,XL,0,0,"ng-template",2),h()())},dependencies:[Ki],styles:[`.mat-mdc-dialog-container {
  width: 100%;
  height: 100%;
  display: block;
  box-sizing: border-box;
  max-height: inherit;
  min-height: inherit;
  min-width: inherit;
  max-width: inherit;
  outline: 0;
}

.cdk-overlay-pane.mat-mdc-dialog-panel {
  max-width: var(--mat-dialog-container-max-width, 560px);
  min-width: var(--mat-dialog-container-min-width, 280px);
}
@media (max-width: 599px) {
  .cdk-overlay-pane.mat-mdc-dialog-panel {
    max-width: var(--mat-dialog-container-small-max-width, calc(100vw - 32px));
  }
}

.mat-mdc-dialog-inner-container {
  display: flex;
  flex-direction: row;
  align-items: center;
  justify-content: space-around;
  box-sizing: border-box;
  height: 100%;
  opacity: 0;
  transition: opacity linear var(--mat-dialog-transition-duration, 0ms);
  max-height: inherit;
  min-height: inherit;
  min-width: inherit;
  max-width: inherit;
}
.mdc-dialog--closing .mat-mdc-dialog-inner-container {
  transition: opacity 75ms linear;
  transform: none;
}
.mdc-dialog--open .mat-mdc-dialog-inner-container {
  opacity: 1;
}
._mat-animation-noopable .mat-mdc-dialog-inner-container {
  transition: none;
}

.mat-mdc-dialog-surface {
  display: flex;
  flex-direction: column;
  flex-grow: 0;
  flex-shrink: 0;
  box-sizing: border-box;
  width: 100%;
  height: 100%;
  position: relative;
  overflow-y: auto;
  outline: 0;
  transform: scale(0.8);
  transition: transform var(--mat-dialog-transition-duration, 0ms) cubic-bezier(0, 0, 0.2, 1);
  max-height: inherit;
  min-height: inherit;
  min-width: inherit;
  max-width: inherit;
  box-shadow: var(--mat-dialog-container-elevation-shadow, none);
  border-radius: var(--mat-dialog-container-shape, var(--mat-sys-corner-extra-large, 4px));
  background-color: var(--mat-dialog-container-color, var(--mat-sys-surface, white));
}
[dir=rtl] .mat-mdc-dialog-surface {
  text-align: right;
}
.mdc-dialog--open .mat-mdc-dialog-surface, .mdc-dialog--closing .mat-mdc-dialog-surface {
  transform: none;
}
._mat-animation-noopable .mat-mdc-dialog-surface {
  transition: none;
}
.mat-mdc-dialog-surface::before {
  position: absolute;
  box-sizing: border-box;
  width: 100%;
  height: 100%;
  top: 0;
  left: 0;
  border: 2px solid transparent;
  border-radius: inherit;
  content: "";
  pointer-events: none;
}

.mat-mdc-dialog-title {
  display: block;
  position: relative;
  flex-shrink: 0;
  box-sizing: border-box;
  margin: 0 0 1px;
  padding: var(--mat-dialog-headline-padding, 6px 24px 13px);
}
.mat-mdc-dialog-title::before {
  display: inline-block;
  width: 0;
  height: 40px;
  content: "";
  vertical-align: 0;
}
[dir=rtl] .mat-mdc-dialog-title {
  text-align: right;
}
.mat-mdc-dialog-container .mat-mdc-dialog-title {
  color: var(--mat-dialog-subhead-color, var(--mat-sys-on-surface, rgba(0, 0, 0, 0.87)));
  font-family: var(--mat-dialog-subhead-font, var(--mat-sys-headline-small-font, inherit));
  line-height: var(--mat-dialog-subhead-line-height, var(--mat-sys-headline-small-line-height, 1.5rem));
  font-size: var(--mat-dialog-subhead-size, var(--mat-sys-headline-small-size, 1rem));
  font-weight: var(--mat-dialog-subhead-weight, var(--mat-sys-headline-small-weight, 400));
  letter-spacing: var(--mat-dialog-subhead-tracking, var(--mat-sys-headline-small-tracking, 0.03125em));
}

.mat-mdc-dialog-content {
  display: block;
  flex-grow: 1;
  box-sizing: border-box;
  margin: 0;
  overflow: auto;
  max-height: 65vh;
}
.mat-mdc-dialog-content > :first-child {
  margin-top: 0;
}
.mat-mdc-dialog-content > :last-child {
  margin-bottom: 0;
}
.mat-mdc-dialog-container .mat-mdc-dialog-content {
  color: var(--mat-dialog-supporting-text-color, var(--mat-sys-on-surface-variant, rgba(0, 0, 0, 0.6)));
  font-family: var(--mat-dialog-supporting-text-font, var(--mat-sys-body-medium-font, inherit));
  line-height: var(--mat-dialog-supporting-text-line-height, var(--mat-sys-body-medium-line-height, 1.5rem));
  font-size: var(--mat-dialog-supporting-text-size, var(--mat-sys-body-medium-size, 1rem));
  font-weight: var(--mat-dialog-supporting-text-weight, var(--mat-sys-body-medium-weight, 400));
  letter-spacing: var(--mat-dialog-supporting-text-tracking, var(--mat-sys-body-medium-tracking, 0.03125em));
}
.mat-mdc-dialog-container .mat-mdc-dialog-content {
  padding: var(--mat-dialog-content-padding, 20px 24px);
}
.mat-mdc-dialog-container-with-actions .mat-mdc-dialog-content {
  padding: var(--mat-dialog-with-actions-content-padding, 20px 24px 0);
}
.mat-mdc-dialog-container .mat-mdc-dialog-title + .mat-mdc-dialog-content {
  padding-top: 0;
}

.mat-mdc-dialog-actions {
  display: flex;
  position: relative;
  flex-shrink: 0;
  flex-wrap: wrap;
  align-items: center;
  box-sizing: border-box;
  min-height: 52px;
  margin: 0;
  border-top: 1px solid transparent;
  padding: var(--mat-dialog-actions-padding, 16px 24px);
  justify-content: var(--mat-dialog-actions-alignment, flex-end);
}
@media (forced-colors: active) {
  .mat-mdc-dialog-actions {
    border-top-color: CanvasText;
  }
}
.mat-mdc-dialog-actions.mat-mdc-dialog-actions-align-start, .mat-mdc-dialog-actions[align=start] {
  justify-content: start;
}
.mat-mdc-dialog-actions.mat-mdc-dialog-actions-align-center, .mat-mdc-dialog-actions[align=center] {
  justify-content: center;
}
.mat-mdc-dialog-actions.mat-mdc-dialog-actions-align-end, .mat-mdc-dialog-actions[align=end] {
  justify-content: flex-end;
}
.mat-mdc-dialog-actions .mat-button-base + .mat-button-base,
.mat-mdc-dialog-actions .mat-mdc-button-base + .mat-mdc-button-base {
  margin-left: 8px;
}
[dir=rtl] .mat-mdc-dialog-actions .mat-button-base + .mat-button-base,
[dir=rtl] .mat-mdc-dialog-actions .mat-mdc-button-base + .mat-mdc-button-base {
  margin-left: 0;
  margin-right: 8px;
}

.mat-mdc-dialog-component-host {
  display: contents;
}
`],encapsulation:2})}return t})(),tI="--mat-dialog-transition-duration";function nI(t){return t==null?null:typeof t=="number"?t:t.endsWith("ms")?Hn(t.substring(0,t.length-2)):t.endsWith("s")?Hn(t.substring(0,t.length-1))*1e3:t==="0"?0:null}var Vf=(function(t){return t[t.OPEN=0]="OPEN",t[t.CLOSING=1]="CLOSING",t[t.CLOSED=2]="CLOSED",t})(Vf||{}),Xl=class{_ref;_config;_containerInstance;componentInstance;componentRef=null;disableClose;id;_afterOpened=new Mi(1);_beforeClosed=new Mi(1);_result;_closeFallbackTimeout;_state=Vf.OPEN;_closeInteractionType;constructor(n,e,i){this._ref=n,this._config=e,this._containerInstance=i,this.disableClose=e.disableClose,this.id=n.id,n.addPanelClass("mat-mdc-dialog-panel"),i._animationStateChanged.pipe(De(r=>r.state==="opened"),ut(1)).subscribe(()=>{this._afterOpened.next(),this._afterOpened.complete()}),i._animationStateChanged.pipe(De(r=>r.state==="closed"),ut(1)).subscribe(()=>{clearTimeout(this._closeFallbackTimeout),this._finishDialogClose()}),n.overlayRef.detachments().subscribe(()=>{this._beforeClosed.next(this._result),this._beforeClosed.complete(),this._finishDialogClose()}),mn(this.backdropClick(),this.keydownEvents().pipe(De(r=>r.keyCode===27&&!this.disableClose&&!Ct(r)))).subscribe(r=>{this.disableClose||(r.preventDefault(),t2(this,r.type==="keydown"?"keyboard":"mouse"))})}close(n){let e=this._config.closePredicate;e&&!e(n,this._config,this.componentInstance)||(this._result=n,this._containerInstance._animationStateChanged.pipe(De(i=>i.state==="closing"),ut(1)).subscribe(i=>{this._beforeClosed.next(n),this._beforeClosed.complete(),this._ref.overlayRef.detachBackdrop(),this._closeFallbackTimeout=setTimeout(()=>this._finishDialogClose(),i.totalTime+100)}),this._state=Vf.CLOSING,this._containerInstance._startExitAnimation())}afterOpened(){return this._afterOpened}afterClosed(){return this._ref.closed}beforeClosed(){return this._beforeClosed}backdropClick(){return this._ref.backdropClick}keydownEvents(){return this._ref.keydownEvents}updatePosition(n){let e=this._ref.config.positionStrategy;return n&&(n.left||n.right)?n.left?e.left(n.left):e.right(n.right):e.centerHorizontally(),n&&(n.top||n.bottom)?n.top?e.top(n.top):e.bottom(n.bottom):e.centerVertically(),this._ref.updatePosition(),this}updateSize(n="",e=""){return this._ref.updateSize(n,e),this}addPanelClass(n){return this._ref.addPanelClass(n),this}removePanelClass(n){return this._ref.removePanelClass(n),this}getState(){return this._state}_finishDialogClose(){this._state=Vf.CLOSED,this._ref.close(this._result,{focusOrigin:this._closeInteractionType}),this.componentInstance=null}};function t2(t,n,e){return t._closeInteractionType=n,t.close(e)}var jb=new y("MatMdcDialogData"),n2=new y("mat-mdc-dialog-default-options"),i2=new y("mat-mdc-dialog-scroll-strategy",{providedIn:"root",factory:()=>{let t=d(X);return()=>ql(t)}}),iI=(()=>{class t{_defaultOptions=d(n2,{optional:!0});_scrollStrategy=d(i2);_parentDialog=d(t,{optional:!0,skipSelf:!0});_idGenerator=d(Ie);_injector=d(X);_dialog=d(KS);_animationsDisabled=ke();_openDialogsAtThisLevel=[];_afterAllClosedAtThisLevel=new I;_afterOpenedAtThisLevel=new I;dialogConfigClass=Hf;_dialogRefConstructor;_dialogContainerType;_dialogDataToken;get openDialogs(){return this._parentDialog?this._parentDialog.openDialogs:this._openDialogsAtThisLevel}get afterOpened(){return this._parentDialog?this._parentDialog.afterOpened:this._afterOpenedAtThisLevel}_getAfterAllClosed(){let e=this._parentDialog;return e?e._getAfterAllClosed():this._afterAllClosedAtThisLevel}afterAllClosed=ci(()=>this.openDialogs.length?this._getAfterAllClosed():this._getAfterAllClosed().pipe(et(void 0)));constructor(){this._dialogRefConstructor=Xl,this._dialogContainerType=e2,this._dialogDataToken=jb}open(e,i){let r;i=b(b({},this._defaultOptions||new Hf),i),i.id=i.id||this._idGenerator.getId("mat-mdc-dialog-"),i.scrollStrategy=i.scrollStrategy||this._scrollStrategy();let o=this._dialog.open(e,Z(b({},i),{positionStrategy:bs(this._injector).centerHorizontally().centerVertically(),disableClose:!0,closePredicate:void 0,closeOnDestroy:!1,closeOnOverlayDetachments:!1,disableAnimations:this._animationsDisabled||i.enterAnimationDuration?.toLocaleString()==="0"||i.exitAnimationDuration?.toString()==="0",container:{type:this._dialogContainerType,providers:()=>[{provide:this.dialogConfigClass,useValue:i},{provide:uo,useValue:i}]},templateContext:()=>({dialogRef:r}),providers:(s,a,c)=>(r=new this._dialogRefConstructor(s,i,c),r.updatePosition(i?.position),[{provide:this._dialogContainerType,useValue:c},{provide:this._dialogDataToken,useValue:a.data},{provide:this._dialogRefConstructor,useValue:r}])}));return r.componentRef=o.componentRef,r.componentInstance=o.componentInstance,this.openDialogs.push(r),this.afterOpened.next(r),r.afterClosed().subscribe(()=>{let s=this.openDialogs.indexOf(r);s>-1&&(this.openDialogs.splice(s,1),this.openDialogs.length||this._getAfterAllClosed().next())}),r}closeAll(){this._closeDialogs(this.openDialogs)}getDialogById(e){return this.openDialogs.find(i=>i.id===e)}ngOnDestroy(){this._closeDialogs(this._openDialogsAtThisLevel),this._afterAllClosedAtThisLevel.complete(),this._afterOpenedAtThisLevel.complete()}_closeDialogs(e){let i=e.length;for(;i--;)e[i].close()}static \u0275fac=function(i){return new(i||t)};static \u0275prov=C({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();var mo=class{_multiple;_emitChanges;compareWith;_selection=new Set;_deselectedToEmit=[];_selectedToEmit=[];_selected=null;get selected(){return this._selected||(this._selected=Array.from(this._selection.values())),this._selected}changed=new I;constructor(n=!1,e,i=!0,r){this._multiple=n,this._emitChanges=i,this.compareWith=r,e&&e.length&&(n?e.forEach(o=>this._markSelected(o)):this._markSelected(e[0]),this._selectedToEmit.length=0)}select(...n){this._verifyValueAssignment(n),n.forEach(i=>this._markSelected(i));let e=this._hasQueuedChanges();return this._emitChangeEvent(),e}deselect(...n){this._verifyValueAssignment(n),n.forEach(i=>this._unmarkSelected(i));let e=this._hasQueuedChanges();return this._emitChangeEvent(),e}setSelection(...n){this._verifyValueAssignment(n);let e=this.selected,i=new Set(n.map(o=>this._getConcreteValue(o)));n.forEach(o=>this._markSelected(o)),e.filter(o=>!i.has(this._getConcreteValue(o,i))).forEach(o=>this._unmarkSelected(o));let r=this._hasQueuedChanges();return this._emitChangeEvent(),r}toggle(n){return this.isSelected(n)?this.deselect(n):this.select(n)}clear(n=!0){this._unmarkAll();let e=this._hasQueuedChanges();return n&&this._emitChangeEvent(),e}isSelected(n){return this._selection.has(this._getConcreteValue(n))}isEmpty(){return this._selection.size===0}hasValue(){return!this.isEmpty()}sort(n){this._multiple&&this.selected&&this._selected.sort(n)}isMultipleSelection(){return this._multiple}_emitChangeEvent(){this._selected=null,(this._selectedToEmit.length||this._deselectedToEmit.length)&&(this.changed.next({source:this,added:this._selectedToEmit,removed:this._deselectedToEmit}),this._deselectedToEmit=[],this._selectedToEmit=[])}_markSelected(n){n=this._getConcreteValue(n),this.isSelected(n)||(this._multiple||this._unmarkAll(),this.isSelected(n)||this._selection.add(n),this._emitChanges&&this._selectedToEmit.push(n))}_unmarkSelected(n){n=this._getConcreteValue(n),this.isSelected(n)&&(this._selection.delete(n),this._emitChanges&&this._deselectedToEmit.push(n))}_unmarkAll(){this.isEmpty()||this._selection.forEach(n=>this._unmarkSelected(n))}_verifyValueAssignment(n){n.length>1&&this._multiple}_hasQueuedChanges(){return!!(this._deselectedToEmit.length||this._selectedToEmit.length)}_getConcreteValue(n,e){if(this.compareWith){e=e??this._selection;for(let i of e)if(this.compareWith(n,i))return i;return n}else return n}};var Bb=(()=>{class t{_listeners=[];notify(e,i){for(let r of this._listeners)r(e,i)}listen(e){return this._listeners.push(e),()=>{this._listeners=this._listeners.filter(i=>e!==i)}}ngOnDestroy(){this._listeners=[]}static \u0275fac=function(i){return new(i||t)};static \u0275prov=C({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();var uI=(()=>{class t{_renderer;_elementRef;onChange=e=>{};onTouched=()=>{};constructor(e,i){this._renderer=e,this._elementRef=i}setProperty(e,i){this._renderer.setProperty(this._elementRef.nativeElement,e,i)}registerOnTouched(e){this.onTouched=e}registerOnChange(e){this.onChange=e}setDisabledState(e){this.setProperty("disabled",e)}static \u0275fac=function(i){return new(i||t)(ee(xe),ee(L))};static \u0275dir=A({type:t})}return t})(),mI=(()=>{class t extends uI{static \u0275fac=(()=>{let e;return function(r){return(e||(e=ct(t)))(r||t)}})();static \u0275dir=A({type:t,features:[fe]})}return t})(),Ei=new y("");var r2={provide:Ei,useExisting:It(()=>ho),multi:!0};function o2(){let t=Jn()?Jn().getUserAgent():"";return/android (\d+)/.test(t.toLowerCase())}var s2=new y(""),ho=(()=>{class t extends uI{_compositionMode;_composing=!1;constructor(e,i,r){super(e,i),this._compositionMode=r,this._compositionMode==null&&(this._compositionMode=!o2())}writeValue(e){let i=e??"";this.setProperty("value",i)}_handleInput(e){(!this._compositionMode||this._compositionMode&&!this._composing)&&this.onChange(e)}_compositionStart(){this._composing=!0}_compositionEnd(e){this._composing=!1,this._compositionMode&&this.onChange(e)}static \u0275fac=function(i){return new(i||t)(ee(xe),ee(L),ee(s2,8))};static \u0275dir=A({type:t,selectors:[["input","formControlName","",3,"type","checkbox"],["textarea","formControlName",""],["input","formControl","",3,"type","checkbox"],["textarea","formControl",""],["input","ngModel","",3,"type","checkbox"],["textarea","ngModel",""],["","ngDefaultControl",""]],hostBindings:function(i,r){i&1&&T("input",function(s){return r._handleInput(s.target.value)})("blur",function(){return r.onTouched()})("compositionstart",function(){return r._compositionStart()})("compositionend",function(s){return r._compositionEnd(s.target.value)})},standalone:!1,features:[Ee([r2]),fe]})}return t})();function zb(t){return t==null||$b(t)===0}function $b(t){return t==null?null:Array.isArray(t)||typeof t=="string"?t.length:t instanceof Set?t.size:null}var Si=new y(""),sd=new y(""),a2=/^(?=.{1,254}$)(?=.{1,64}@)[a-zA-Z0-9!#$%&'*+/=?^_`{|}~-]+(?:\.[a-zA-Z0-9!#$%&'*+/=?^_`{|}~-]+)*@[a-zA-Z0-9](?:[a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?(?:\.[a-zA-Z0-9](?:[a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?)*$/,Jt=class{static min(n){return fI(n)}static max(n){return c2(n)}static required(n){return hI(n)}static requiredTrue(n){return l2(n)}static email(n){return d2(n)}static minLength(n){return u2(n)}static maxLength(n){return m2(n)}static pattern(n){return f2(n)}static nullValidator(n){return zf()}static compose(n){return yI(n)}static composeAsync(n){return xI(n)}};function fI(t){return n=>{if(n.value==null||t==null)return null;let e=parseFloat(n.value);return!isNaN(e)&&e<t?{min:{min:t,actual:n.value}}:null}}function c2(t){return n=>{if(n.value==null||t==null)return null;let e=parseFloat(n.value);return!isNaN(e)&&e>t?{max:{max:t,actual:n.value}}:null}}function hI(t){return zb(t.value)?{required:!0}:null}function l2(t){return t.value===!0?null:{required:!0}}function d2(t){return zb(t.value)||a2.test(t.value)?null:{email:!0}}function u2(t){return n=>{let e=n.value?.length??$b(n.value);return e===null||e===0?null:e<t?{minlength:{requiredLength:t,actualLength:e}}:null}}function m2(t){return n=>{let e=n.value?.length??$b(n.value);return e!==null&&e>t?{maxlength:{requiredLength:t,actualLength:e}}:null}}function f2(t){if(!t)return zf;let n,e;return typeof t=="string"?(e="",t.charAt(0)!=="^"&&(e+="^"),e+=t,t.charAt(t.length-1)!=="$"&&(e+="$"),n=new RegExp(e)):(e=t.toString(),n=t),i=>{if(zb(i.value))return null;let r=i.value;return n.test(r)?null:{pattern:{requiredPattern:e,actualValue:r}}}}function zf(t){return null}function pI(t){return t!=null}function gI(t){return Xr(t)?ot(t):t}function _I(t){let n={};return t.forEach(e=>{n=e!=null?b(b({},n),e):n}),Object.keys(n).length===0?null:n}function vI(t,n){return n.map(e=>e(t))}function h2(t){return!t.validate}function bI(t){return t.map(n=>h2(n)?n:e=>n.validate(e))}function yI(t){if(!t)return null;let n=t.filter(pI);return n.length==0?null:function(e){return _I(vI(e,n))}}function Gb(t){return t!=null?yI(bI(t)):null}function xI(t){if(!t)return null;let n=t.filter(pI);return n.length==0?null:function(e){let i=vI(e,n).map(gI);return Oo(i).pipe(Q(_I))}}function Wb(t){return t!=null?xI(bI(t)):null}function rI(t,n){return t===null?[n]:Array.isArray(t)?[...t,n]:[t,n]}function CI(t){return t._rawValidators}function wI(t){return t._rawAsyncValidators}function Vb(t){return t?Array.isArray(t)?t:[t]:[]}function $f(t,n){return Array.isArray(t)?t.includes(n):t===n}function oI(t,n){let e=Vb(n);return Vb(t).forEach(r=>{$f(e,r)||e.push(r)}),e}function sI(t,n){return Vb(n).filter(e=>!$f(t,e))}var Gf=class{get value(){return this.control?this.control.value:null}get valid(){return this.control?this.control.valid:null}get invalid(){return this.control?this.control.invalid:null}get pending(){return this.control?this.control.pending:null}get disabled(){return this.control?this.control.disabled:null}get enabled(){return this.control?this.control.enabled:null}get errors(){return this.control?this.control.errors:null}get pristine(){return this.control?this.control.pristine:null}get dirty(){return this.control?this.control.dirty:null}get touched(){return this.control?this.control.touched:null}get status(){return this.control?this.control.status:null}get untouched(){return this.control?this.control.untouched:null}get statusChanges(){return this.control?this.control.statusChanges:null}get valueChanges(){return this.control?this.control.valueChanges:null}get path(){return null}_composedValidatorFn;_composedAsyncValidatorFn;_rawValidators=[];_rawAsyncValidators=[];_setValidators(n){this._rawValidators=n||[],this._composedValidatorFn=Gb(this._rawValidators)}_setAsyncValidators(n){this._rawAsyncValidators=n||[],this._composedAsyncValidatorFn=Wb(this._rawAsyncValidators)}get validator(){return this._composedValidatorFn||null}get asyncValidator(){return this._composedAsyncValidatorFn||null}_onDestroyCallbacks=[];_registerOnDestroy(n){this._onDestroyCallbacks.push(n)}_invokeOnDestroyCallbacks(){this._onDestroyCallbacks.forEach(n=>n()),this._onDestroyCallbacks=[]}reset(n=void 0){this.control?.reset(n)}hasError(n,e){return this.control?this.control.hasError(n,e):!1}getError(n,e){return this.control?this.control.getError(n,e):null}},Di=class extends Gf{name;get formDirective(){return null}get path(){return null}},Cn=class extends Gf{_parent=null;name=null;valueAccessor=null},Wf=class{_cd;constructor(n){this._cd=n}get isTouched(){return this._cd?.control?._touched?.(),!!this._cd?.control?.touched}get isUntouched(){return!!this._cd?.control?.untouched}get isPristine(){return this._cd?.control?._pristine?.(),!!this._cd?.control?.pristine}get isDirty(){return!!this._cd?.control?.dirty}get isValid(){return this._cd?.control?._status?.(),!!this._cd?.control?.valid}get isInvalid(){return!!this._cd?.control?.invalid}get isPending(){return!!this._cd?.control?.pending}get isSubmitted(){return this._cd?._submitted?.(),!!this._cd?.submitted}};var ii=(()=>{class t extends Wf{constructor(e){super(e)}static \u0275fac=function(i){return new(i||t)(ee(Cn,2))};static \u0275dir=A({type:t,selectors:[["","formControlName",""],["","ngModel",""],["","formControl",""]],hostVars:14,hostBindings:function(i,r){i&2&&z("ng-untouched",r.isUntouched)("ng-touched",r.isTouched)("ng-pristine",r.isPristine)("ng-dirty",r.isDirty)("ng-valid",r.isValid)("ng-invalid",r.isInvalid)("ng-pending",r.isPending)},standalone:!1,features:[fe]})}return t})(),Ba=(()=>{class t extends Wf{constructor(e){super(e)}static \u0275fac=function(i){return new(i||t)(ee(Di,10))};static \u0275dir=A({type:t,selectors:[["","formGroupName",""],["","formArrayName",""],["","ngModelGroup",""],["","formGroup",""],["","formArray",""],["form",3,"ngNoForm",""],["","ngForm",""]],hostVars:16,hostBindings:function(i,r){i&2&&z("ng-untouched",r.isUntouched)("ng-touched",r.isTouched)("ng-pristine",r.isPristine)("ng-dirty",r.isDirty)("ng-valid",r.isValid)("ng-invalid",r.isInvalid)("ng-pending",r.isPending)("ng-submitted",r.isSubmitted)},standalone:!1,features:[fe]})}return t})();var Kl="VALID",Uf="INVALID",Pa="PENDING",Jl="DISABLED",fo=class{},qf=class extends fo{value;source;constructor(n,e){super(),this.value=n,this.source=e}},td=class extends fo{pristine;source;constructor(n,e){super(),this.pristine=n,this.source=e}},nd=class extends fo{touched;source;constructor(n,e){super(),this.touched=n,this.source=e}},Fa=class extends fo{status;source;constructor(n,e){super(),this.status=n,this.source=e}},Yf=class extends fo{source;constructor(n){super(),this.source=n}},rd=class extends fo{source;constructor(n){super(),this.source=n}};function qb(t){return(Kf(t)?t.validators:t)||null}function p2(t){return Array.isArray(t)?Gb(t):t||null}function Yb(t,n){return(Kf(n)?n.asyncValidators:t)||null}function g2(t){return Array.isArray(t)?Wb(t):t||null}function Kf(t){return t!=null&&!Array.isArray(t)&&typeof t=="object"}function DI(t,n,e){let i=t.controls;if(!(n?Object.keys(i):i).length)throw new R(1e3,"");if(!i[e])throw new R(1001,"")}function EI(t,n,e){t._forEachChild((i,r)=>{if(e[r]===void 0)throw new R(-1002,"")})}var La=class{_pendingDirty=!1;_hasOwnPendingAsyncValidator=null;_pendingTouched=!1;_onCollectionChange=()=>{};_updateOn;_parent=null;_asyncValidationSubscription;_composedValidatorFn;_composedAsyncValidatorFn;_rawValidators;_rawAsyncValidators;value;constructor(n,e){this._assignValidators(n),this._assignAsyncValidators(e)}get validator(){return this._composedValidatorFn}set validator(n){this._rawValidators=this._composedValidatorFn=n}get asyncValidator(){return this._composedAsyncValidatorFn}set asyncValidator(n){this._rawAsyncValidators=this._composedAsyncValidatorFn=n}get parent(){return this._parent}get status(){return Ne(this.statusReactive)}set status(n){Ne(()=>this.statusReactive.set(n))}_status=xt(()=>this.statusReactive());statusReactive=M(void 0);get valid(){return this.status===Kl}get invalid(){return this.status===Uf}get pending(){return this.status===Pa}get disabled(){return this.status===Jl}get enabled(){return this.status!==Jl}errors;get pristine(){return Ne(this.pristineReactive)}set pristine(n){Ne(()=>this.pristineReactive.set(n))}_pristine=xt(()=>this.pristineReactive());pristineReactive=M(!0);get dirty(){return!this.pristine}get touched(){return Ne(this.touchedReactive)}set touched(n){Ne(()=>this.touchedReactive.set(n))}_touched=xt(()=>this.touchedReactive());touchedReactive=M(!1);get untouched(){return!this.touched}_events=new I;events=this._events.asObservable();valueChanges;statusChanges;get updateOn(){return this._updateOn?this._updateOn:this.parent?this.parent.updateOn:"change"}setValidators(n){this._assignValidators(n)}setAsyncValidators(n){this._assignAsyncValidators(n)}addValidators(n){this.setValidators(oI(n,this._rawValidators))}addAsyncValidators(n){this.setAsyncValidators(oI(n,this._rawAsyncValidators))}removeValidators(n){this.setValidators(sI(n,this._rawValidators))}removeAsyncValidators(n){this.setAsyncValidators(sI(n,this._rawAsyncValidators))}hasValidator(n){return $f(this._rawValidators,n)}hasAsyncValidator(n){return $f(this._rawAsyncValidators,n)}clearValidators(){this.validator=null}clearAsyncValidators(){this.asyncValidator=null}markAsTouched(n={}){let e=this.touched===!1;this.touched=!0;let i=n.sourceControl??this;n.onlySelf||this._parent?.markAsTouched(Z(b({},n),{sourceControl:i})),e&&n.emitEvent!==!1&&this._events.next(new nd(!0,i))}markAllAsDirty(n={}){this.markAsDirty({onlySelf:!0,emitEvent:n.emitEvent,sourceControl:this}),this._forEachChild(e=>e.markAllAsDirty(n))}markAllAsTouched(n={}){this.markAsTouched({onlySelf:!0,emitEvent:n.emitEvent,sourceControl:this}),this._forEachChild(e=>e.markAllAsTouched(n))}markAsUntouched(n={}){let e=this.touched===!0;this.touched=!1,this._pendingTouched=!1;let i=n.sourceControl??this;this._forEachChild(r=>{r.markAsUntouched({onlySelf:!0,emitEvent:n.emitEvent,sourceControl:i})}),n.onlySelf||this._parent?._updateTouched(n,i),e&&n.emitEvent!==!1&&this._events.next(new nd(!1,i))}markAsDirty(n={}){let e=this.pristine===!0;this.pristine=!1;let i=n.sourceControl??this;n.onlySelf||this._parent?.markAsDirty(Z(b({},n),{sourceControl:i})),e&&n.emitEvent!==!1&&this._events.next(new td(!1,i))}markAsPristine(n={}){let e=this.pristine===!1;this.pristine=!0,this._pendingDirty=!1;let i=n.sourceControl??this;this._forEachChild(r=>{r.markAsPristine({onlySelf:!0,emitEvent:n.emitEvent})}),n.onlySelf||this._parent?._updatePristine(n,i),e&&n.emitEvent!==!1&&this._events.next(new td(!0,i))}markAsPending(n={}){this.status=Pa;let e=n.sourceControl??this;n.emitEvent!==!1&&(this._events.next(new Fa(this.status,e)),this.statusChanges.emit(this.status)),n.onlySelf||this._parent?.markAsPending(Z(b({},n),{sourceControl:e}))}disable(n={}){let e=this._parentMarkedDirty(n.onlySelf);this.status=Jl,this.errors=null,this._forEachChild(r=>{r.disable(Z(b({},n),{onlySelf:!0}))}),this._updateValue();let i=n.sourceControl??this;n.emitEvent!==!1&&(this._events.next(new qf(this.value,i)),this._events.next(new Fa(this.status,i)),this.valueChanges.emit(this.value),this.statusChanges.emit(this.status)),this._updateAncestors(Z(b({},n),{skipPristineCheck:e}),this),this._onDisabledChange.forEach(r=>r(!0))}enable(n={}){let e=this._parentMarkedDirty(n.onlySelf);this.status=Kl,this._forEachChild(i=>{i.enable(Z(b({},n),{onlySelf:!0}))}),this.updateValueAndValidity({onlySelf:!0,emitEvent:n.emitEvent}),this._updateAncestors(Z(b({},n),{skipPristineCheck:e}),this),this._onDisabledChange.forEach(i=>i(!1))}_updateAncestors(n,e){n.onlySelf||(this._parent?.updateValueAndValidity(n),n.skipPristineCheck||this._parent?._updatePristine({},e),this._parent?._updateTouched({},e))}setParent(n){this._parent=n}getRawValue(){return this.value}updateValueAndValidity(n={}){if(this._setInitialStatus(),this._updateValue(),this.enabled){let i=this._cancelExistingSubscription();this.errors=this._runValidator(),this.status=this._calculateStatus(),(this.status===Kl||this.status===Pa)&&this._runAsyncValidator(i,n.emitEvent)}let e=n.sourceControl??this;n.emitEvent!==!1&&(this._events.next(new qf(this.value,e)),this._events.next(new Fa(this.status,e)),this.valueChanges.emit(this.value),this.statusChanges.emit(this.status)),n.onlySelf||this._parent?.updateValueAndValidity(Z(b({},n),{sourceControl:e}))}_updateTreeValidity(n={emitEvent:!0}){this._forEachChild(e=>e._updateTreeValidity(n)),this.updateValueAndValidity({onlySelf:!0,emitEvent:n.emitEvent})}_setInitialStatus(){this.status=this._allControlsDisabled()?Jl:Kl}_runValidator(){return this.validator?this.validator(this):null}_runAsyncValidator(n,e){if(this.asyncValidator){this.status=Pa,this._hasOwnPendingAsyncValidator={emitEvent:e!==!1,shouldHaveEmitted:n!==!1};let i=gI(this.asyncValidator(this));this._asyncValidationSubscription=i.subscribe(r=>{this._hasOwnPendingAsyncValidator=null,this.setErrors(r,{emitEvent:e,shouldHaveEmitted:n})})}}_cancelExistingSubscription(){if(this._asyncValidationSubscription){this._asyncValidationSubscription.unsubscribe();let n=(this._hasOwnPendingAsyncValidator?.emitEvent||this._hasOwnPendingAsyncValidator?.shouldHaveEmitted)??!1;return this._hasOwnPendingAsyncValidator=null,n}return!1}setErrors(n,e={}){this.errors=n,this._updateControlsErrors(e.emitEvent!==!1,this,e.shouldHaveEmitted)}get(n){let e=n;return e==null||(Array.isArray(e)||(e=e.split(".")),e.length===0)?null:e.reduce((i,r)=>i&&i._find(r),this)}getError(n,e){let i=e?this.get(e):this;return i?.errors?i.errors[n]:null}hasError(n,e){return!!this.getError(n,e)}get root(){let n=this;for(;n._parent;)n=n._parent;return n}_updateControlsErrors(n,e,i){this.status=this._calculateStatus(),n&&this.statusChanges.emit(this.status),(n||i)&&this._events.next(new Fa(this.status,e)),this._parent&&this._parent._updateControlsErrors(n,e,i)}_initObservables(){this.valueChanges=new U,this.statusChanges=new U}_calculateStatus(){return this._allControlsDisabled()?Jl:this.errors?Uf:this._hasOwnPendingAsyncValidator||this._anyControlsHaveStatus(Pa)?Pa:this._anyControlsHaveStatus(Uf)?Uf:Kl}_anyControlsHaveStatus(n){return this._anyControls(e=>e.status===n)}_anyControlsDirty(){return this._anyControls(n=>n.dirty)}_anyControlsTouched(){return this._anyControls(n=>n.touched)}_updatePristine(n,e){let i=!this._anyControlsDirty(),r=this.pristine!==i;this.pristine=i,n.onlySelf||this._parent?._updatePristine(n,e),r&&this._events.next(new td(this.pristine,e))}_updateTouched(n={},e){this.touched=this._anyControlsTouched(),this._events.next(new nd(this.touched,e)),n.onlySelf||this._parent?._updateTouched(n,e)}_onDisabledChange=[];_registerOnCollectionChange(n){this._onCollectionChange=n}_setUpdateStrategy(n){Kf(n)&&n.updateOn!=null&&(this._updateOn=n.updateOn)}_parentMarkedDirty(n){return!n&&!!this._parent?.dirty&&!this._parent._anyControlsDirty()}_find(n){return null}_assignValidators(n){this._rawValidators=Array.isArray(n)?n.slice():n,this._composedValidatorFn=p2(this._rawValidators)}_assignAsyncValidators(n){this._rawAsyncValidators=Array.isArray(n)?n.slice():n,this._composedAsyncValidatorFn=g2(this._rawAsyncValidators)}},ja=class extends La{constructor(n,e,i){super(qb(e),Yb(i,e)),this.controls=n,this._initObservables(),this._setUpdateStrategy(e),this._setUpControls(),this.updateValueAndValidity({onlySelf:!0,emitEvent:!!this.asyncValidator})}controls;registerControl(n,e){return this.controls[n]?this.controls[n]:(this.controls[n]=e,e.setParent(this),e._registerOnCollectionChange(this._onCollectionChange),e)}addControl(n,e,i={}){this.registerControl(n,e),this.updateValueAndValidity({emitEvent:i.emitEvent}),this._onCollectionChange()}removeControl(n,e={}){this.controls[n]&&this.controls[n]._registerOnCollectionChange(()=>{}),delete this.controls[n],this.updateValueAndValidity({emitEvent:e.emitEvent}),this._onCollectionChange()}setControl(n,e,i={}){this.controls[n]&&this.controls[n]._registerOnCollectionChange(()=>{}),delete this.controls[n],e&&this.registerControl(n,e),this.updateValueAndValidity({emitEvent:i.emitEvent}),this._onCollectionChange()}contains(n){return this.controls.hasOwnProperty(n)&&this.controls[n].enabled}setValue(n,e={}){EI(this,!0,n),Object.keys(n).forEach(i=>{DI(this,!0,i),this.controls[i].setValue(n[i],{onlySelf:!0,emitEvent:e.emitEvent})}),this.updateValueAndValidity(e)}patchValue(n,e={}){n!=null&&(Object.keys(n).forEach(i=>{let r=this.controls[i];r&&r.patchValue(n[i],{onlySelf:!0,emitEvent:e.emitEvent})}),this.updateValueAndValidity(e))}reset(n={},e={}){this._forEachChild((i,r)=>{i.reset(n?n[r]:null,Z(b({},e),{onlySelf:!0}))}),this._updatePristine(e,this),this._updateTouched(e,this),this.updateValueAndValidity(e),e?.emitEvent!==!1&&this._events.next(new rd(this))}getRawValue(){return this._reduceChildren({},(n,e,i)=>(n[i]=e.getRawValue(),n))}_syncPendingControls(){let n=this._reduceChildren(!1,(e,i)=>i._syncPendingControls()?!0:e);return n&&this.updateValueAndValidity({onlySelf:!0}),n}_forEachChild(n){Object.keys(this.controls).forEach(e=>{let i=this.controls[e];i&&n(i,e)})}_setUpControls(){this._forEachChild(n=>{n.setParent(this),n._registerOnCollectionChange(this._onCollectionChange)})}_updateValue(){this.value=this._reduceValue()}_anyControls(n){for(let[e,i]of Object.entries(this.controls))if(this.contains(e)&&n(i))return!0;return!1}_reduceValue(){let n={};return this._reduceChildren(n,(e,i,r)=>((i.enabled||this.disabled)&&(e[r]=i.value),e))}_reduceChildren(n,e){let i=n;return this._forEachChild((r,o)=>{i=e(i,r,o)}),i}_allControlsDisabled(){for(let n of Object.keys(this.controls))if(this.controls[n].enabled)return!1;return Object.keys(this.controls).length>0||this.disabled}_find(n){return this.controls.hasOwnProperty(n)?this.controls[n]:null}};var Hb=class extends ja{};var Va=new y("",{factory:()=>Jf}),Jf="always";function SI(t,n){return[...n.path,t]}function od(t,n,e=Jf){Zb(t,n),n.valueAccessor.writeValue(t.value),(t.disabled||e==="always")&&n.valueAccessor.setDisabledState?.(t.disabled),v2(t,n),y2(t,n),b2(t,n),_2(t,n)}function Zf(t,n,e=!0){let i=()=>{};n?.valueAccessor?.registerOnChange(i),n?.valueAccessor?.registerOnTouched(i),Xf(t,n),t&&(n._invokeOnDestroyCallbacks(),t._registerOnCollectionChange(()=>{}))}function Qf(t,n){t.forEach(e=>{e.registerOnValidatorChange&&e.registerOnValidatorChange(n)})}function _2(t,n){if(n.valueAccessor.setDisabledState){let e=i=>{n.valueAccessor.setDisabledState(i)};t.registerOnDisabledChange(e),n._registerOnDestroy(()=>{t._unregisterOnDisabledChange(e)})}}function Zb(t,n){let e=CI(t);n.validator!==null?t.setValidators(rI(e,n.validator)):typeof e=="function"&&t.setValidators([e]);let i=wI(t);n.asyncValidator!==null?t.setAsyncValidators(rI(i,n.asyncValidator)):typeof i=="function"&&t.setAsyncValidators([i]);let r=()=>t.updateValueAndValidity();Qf(n._rawValidators,r),Qf(n._rawAsyncValidators,r)}function Xf(t,n){let e=!1;if(t!==null){if(n.validator!==null){let r=CI(t);if(Array.isArray(r)&&r.length>0){let o=r.filter(s=>s!==n.validator);o.length!==r.length&&(e=!0,t.setValidators(o))}}if(n.asyncValidator!==null){let r=wI(t);if(Array.isArray(r)&&r.length>0){let o=r.filter(s=>s!==n.asyncValidator);o.length!==r.length&&(e=!0,t.setAsyncValidators(o))}}}let i=()=>{};return Qf(n._rawValidators,i),Qf(n._rawAsyncValidators,i),e}function v2(t,n){n.valueAccessor.registerOnChange(e=>{t._pendingValue=e,t._pendingChange=!0,t._pendingDirty=!0,t.updateOn==="change"&&II(t,n)})}function b2(t,n){n.valueAccessor.registerOnTouched(()=>{t._pendingTouched=!0,t.updateOn==="blur"&&t._pendingChange&&II(t,n),t.updateOn!=="submit"&&t.markAsTouched()})}function II(t,n){t._pendingDirty&&t.markAsDirty(),t.setValue(t._pendingValue,{emitModelToViewChange:!1}),n.viewToModelUpdate(t._pendingValue),t._pendingChange=!1}function y2(t,n){let e=(i,r)=>{n.valueAccessor.writeValue(i),r&&n.viewToModelUpdate(i)};t.registerOnChange(e),n._registerOnDestroy(()=>{t._unregisterOnChange(e)})}function kI(t,n){t==null,Zb(t,n)}function x2(t,n){return Xf(t,n)}function Qb(t,n){if(!t.hasOwnProperty("model"))return!1;let e=t.model;return e.isFirstChange()?!0:!Object.is(n,e.currentValue)}function C2(t){return Object.getPrototypeOf(t.constructor)===mI}function MI(t,n){t._syncPendingControls(),n.forEach(e=>{let i=e.control;i.updateOn==="submit"&&i._pendingChange&&(e.viewToModelUpdate(i._pendingValue),i._pendingChange=!1)})}function Xb(t,n){if(!n)return null;Array.isArray(n);let e,i,r;return n.forEach(o=>{o.constructor===ho?e=o:C2(o)?i=o:r=o}),r||i||e||null}function w2(t,n){let e=t.indexOf(n);e>-1&&t.splice(e,1)}var D2={provide:Di,useExisting:It(()=>ys)},ed=Promise.resolve(),ys=(()=>{class t extends Di{callSetDisabledState;get submitted(){return Ne(this.submittedReactive)}_submitted=xt(()=>this.submittedReactive());submittedReactive=M(!1);_directives=new Set;form;ngSubmit=new U;options;constructor(e,i,r){super(),this.callSetDisabledState=r,this.form=new ja({},Gb(e),Wb(i))}ngAfterViewInit(){this._setUpdateStrategy()}get formDirective(){return this}get control(){return this.form}get path(){return[]}get controls(){return this.form.controls}addControl(e){ed.then(()=>{let i=this._findContainer(e.path);e.control=i.registerControl(e.name,e.control),od(e.control,e,this.callSetDisabledState),e.control.updateValueAndValidity({emitEvent:!1}),this._directives.add(e)})}getControl(e){return this.form.get(e.path)}removeControl(e){ed.then(()=>{this._findContainer(e.path)?.removeControl(e.name),this._directives.delete(e)})}addFormGroup(e){ed.then(()=>{let i=this._findContainer(e.path),r=new ja({});kI(r,e),i.registerControl(e.name,r),r.updateValueAndValidity({emitEvent:!1})})}removeFormGroup(e){ed.then(()=>{this._findContainer(e.path)?.removeControl?.(e.name)})}getFormGroup(e){return this.form.get(e.path)}updateModel(e,i){ed.then(()=>{this.form.get(e.path).setValue(i)})}setValue(e){this.control.setValue(e)}onSubmit(e){return this.submittedReactive.set(!0),MI(this.form,this._directives),this.ngSubmit.emit(e),this.form._events.next(new Yf(this.control)),e?.target?.method==="dialog"}onReset(){this.resetForm()}resetForm(e=void 0){this.form.reset(e),this.submittedReactive.set(!1)}_setUpdateStrategy(){this.options&&this.options.updateOn!=null&&(this.form._updateOn=this.options.updateOn)}_findContainer(e){return e.pop(),e.length?this.form.get(e):this.form}static \u0275fac=function(i){return new(i||t)(ee(Si,10),ee(sd,10),ee(Va,8))};static \u0275dir=A({type:t,selectors:[["form",3,"ngNoForm","",3,"formGroup","",3,"formArray",""],["ng-form"],["","ngForm",""]],hostBindings:function(i,r){i&1&&T("submit",function(s){return r.onSubmit(s)})("reset",function(){return r.onReset()})},inputs:{options:[0,"ngFormOptions","options"]},outputs:{ngSubmit:"ngSubmit"},exportAs:["ngForm"],standalone:!1,features:[Ee([D2]),fe]})}return t})();function aI(t,n){let e=t.indexOf(n);e>-1&&t.splice(e,1)}function cI(t){return typeof t=="object"&&t!==null&&Object.keys(t).length===2&&"value"in t&&"disabled"in t}var id=class extends La{defaultValue=null;_onChange=[];_pendingValue;_pendingChange=!1;constructor(n=null,e,i){super(qb(e),Yb(i,e)),this._applyFormState(n),this._setUpdateStrategy(e),this._initObservables(),this.updateValueAndValidity({onlySelf:!0,emitEvent:!!this.asyncValidator}),Kf(e)&&(e.nonNullable||e.initialValueIsDefault)&&(cI(n)?this.defaultValue=n.value:this.defaultValue=n)}setValue(n,e={}){this.value=this._pendingValue=n,this._onChange.length&&e.emitModelToViewChange!==!1&&this._onChange.forEach(i=>i(this.value,e.emitViewToModelChange!==!1)),this.updateValueAndValidity(e)}patchValue(n,e={}){this.setValue(n,e)}reset(n=this.defaultValue,e={}){this._applyFormState(n),this.markAsPristine(e),this.markAsUntouched(e),this.setValue(this.value,e),e.overwriteDefaultValue&&(this.defaultValue=this.value),this._pendingChange=!1,e?.emitEvent!==!1&&this._events.next(new rd(this))}_updateValue(){}_anyControls(n){return!1}_allControlsDisabled(){return this.disabled}registerOnChange(n){this._onChange.push(n)}_unregisterOnChange(n){aI(this._onChange,n)}registerOnDisabledChange(n){this._onDisabledChange.push(n)}_unregisterOnDisabledChange(n){aI(this._onDisabledChange,n)}_forEachChild(n){}_syncPendingControls(){return this.updateOn==="submit"&&(this._pendingDirty&&this.markAsDirty(),this._pendingTouched&&this.markAsTouched(),this._pendingChange)?(this.setValue(this._pendingValue,{onlySelf:!0,emitModelToViewChange:!1}),!0):!1}_applyFormState(n){cI(n)?(this.value=this._pendingValue=n.value,n.disabled?this.disable({onlySelf:!0,emitEvent:!1}):this.enable({onlySelf:!0,emitEvent:!1})):this.value=this._pendingValue=n}};var E2=t=>t instanceof id;var S2={provide:Cn,useExisting:It(()=>xs)},lI=Promise.resolve(),xs=(()=>{class t extends Cn{_changeDetectorRef;callSetDisabledState;control=new id;static ngAcceptInputType_isDisabled;_registered=!1;viewModel;name="";isDisabled;model;options;update=new U;constructor(e,i,r,o,s,a){super(),this._changeDetectorRef=s,this.callSetDisabledState=a,this._parent=e,this._setValidators(i),this._setAsyncValidators(r),this.valueAccessor=Xb(this,o)}ngOnChanges(e){if(this._checkForErrors(),!this._registered||"name"in e){if(this._registered&&(this._checkName(),this.formDirective)){let i=e.name.previousValue;this.formDirective.removeControl({name:i,path:this._getPath(i)})}this._setUpControl()}"isDisabled"in e&&this._updateDisabled(e),Qb(e,this.viewModel)&&(this._updateValue(this.model),this.viewModel=this.model)}ngOnDestroy(){this.formDirective?.removeControl(this)}get path(){return this._getPath(this.name)}get formDirective(){return this._parent?this._parent.formDirective:null}viewToModelUpdate(e){this.viewModel=e,this.update.emit(e)}_setUpControl(){this._setUpdateStrategy(),this._isStandalone()?this._setUpStandalone():this.formDirective.addControl(this),this._registered=!0}_setUpdateStrategy(){this.options&&this.options.updateOn!=null&&(this.control._updateOn=this.options.updateOn)}_isStandalone(){return!this._parent||!!(this.options&&this.options.standalone)}_setUpStandalone(){od(this.control,this,this.callSetDisabledState),this.control.updateValueAndValidity({emitEvent:!1})}_checkForErrors(){this._checkName()}_checkName(){this.options&&this.options.name&&(this.name=this.options.name),!this._isStandalone()&&this.name}_updateValue(e){lI.then(()=>{this.control.setValue(e,{emitViewToModelChange:!1}),this._changeDetectorRef?.markForCheck()})}_updateDisabled(e){let i=e.isDisabled.currentValue,r=i!==0&&B(i);lI.then(()=>{r&&!this.control.disabled?this.control.disable():!r&&this.control.disabled&&this.control.enable(),this._changeDetectorRef?.markForCheck()})}_getPath(e){return this._parent?SI(e,this._parent):[e]}static \u0275fac=function(i){return new(i||t)(ee(Di,9),ee(Si,10),ee(sd,10),ee(Ei,10),ee(ve,8),ee(Va,8))};static \u0275dir=A({type:t,selectors:[["","ngModel","",3,"formControlName","",3,"formControl",""]],inputs:{name:"name",isDisabled:[0,"disabled","isDisabled"],model:[0,"ngModel","model"],options:[0,"ngModelOptions","options"]},outputs:{update:"ngModelChange"},exportAs:["ngModel"],standalone:!1,features:[Ee([S2]),fe,ze]})}return t})();var Ha=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275dir=A({type:t,selectors:[["form",3,"ngNoForm","",3,"ngNativeValidate",""]],hostAttrs:["novalidate",""],standalone:!1})}return t})(),I2={provide:Ei,useExisting:It(()=>Kb),multi:!0},Kb=(()=>{class t extends mI{writeValue(e){let i=e??"";this.setProperty("value",i)}registerOnChange(e){this.onChange=i=>{e(i==""?null:parseFloat(i))}}static \u0275fac=(()=>{let e;return function(r){return(e||(e=ct(t)))(r||t)}})();static \u0275dir=A({type:t,selectors:[["input","type","number","formControlName",""],["input","type","number","formControl",""],["input","type","number","ngModel",""]],hostBindings:function(i,r){i&1&&T("input",function(s){return r.onChange(s.target.value)})("blur",function(){return r.onTouched()})},standalone:!1,features:[Ee([I2]),fe]})}return t})();var Ub=class extends La{constructor(n,e,i){super(qb(e),Yb(i,e)),this.controls=n,this._initObservables(),this._setUpdateStrategy(e),this._setUpControls(),this.updateValueAndValidity({onlySelf:!0,emitEvent:!!this.asyncValidator})}controls;at(n){return this.controls[this._adjustIndex(n)]}push(n,e={}){Array.isArray(n)?n.forEach(i=>{this.controls.push(i),this._registerControl(i)}):(this.controls.push(n),this._registerControl(n)),this.updateValueAndValidity({emitEvent:e.emitEvent}),this._onCollectionChange()}insert(n,e,i={}){this.controls.splice(n,0,e),this._registerControl(e),this.updateValueAndValidity({emitEvent:i.emitEvent})}removeAt(n,e={}){let i=this._adjustIndex(n);i<0&&(i=0),this.controls[i]&&this.controls[i]._registerOnCollectionChange(()=>{}),this.controls.splice(i,1),this.updateValueAndValidity({emitEvent:e.emitEvent})}setControl(n,e,i={}){let r=this._adjustIndex(n);r<0&&(r=0),this.controls[r]&&this.controls[r]._registerOnCollectionChange(()=>{}),this.controls.splice(r,1),e&&(this.controls.splice(r,0,e),this._registerControl(e)),this.updateValueAndValidity({emitEvent:i.emitEvent}),this._onCollectionChange()}get length(){return this.controls.length}setValue(n,e={}){EI(this,!1,n),n.forEach((i,r)=>{DI(this,!1,r),this.at(r).setValue(i,{onlySelf:!0,emitEvent:e.emitEvent})}),this.updateValueAndValidity(e)}patchValue(n,e={}){n!=null&&(n.forEach((i,r)=>{this.at(r)&&this.at(r).patchValue(i,{onlySelf:!0,emitEvent:e.emitEvent})}),this.updateValueAndValidity(e))}reset(n=[],e={}){this._forEachChild((i,r)=>{i.reset(n[r],Z(b({},e),{onlySelf:!0}))}),this._updatePristine(e,this),this._updateTouched(e,this),this.updateValueAndValidity(e),e?.emitEvent!==!1&&this._events.next(new rd(this))}getRawValue(){return this.controls.map(n=>n.getRawValue())}clear(n={}){this.controls.length<1||(this._forEachChild(e=>e._registerOnCollectionChange(()=>{})),this.controls.splice(0),this.updateValueAndValidity({emitEvent:n.emitEvent}))}_adjustIndex(n){return n<0?n+this.length:n}_syncPendingControls(){let n=this.controls.reduce((e,i)=>i._syncPendingControls()?!0:e,!1);return n&&this.updateValueAndValidity({onlySelf:!0}),n}_forEachChild(n){this.controls.forEach((e,i)=>{n(e,i)})}_updateValue(){this.value=this.controls.filter(n=>n.enabled||this.disabled).map(n=>n.value)}_anyControls(n){return this.controls.some(e=>e.enabled&&n(e))}_setUpControls(){this._forEachChild(n=>this._registerControl(n))}_allControlsDisabled(){for(let n of this.controls)if(n.enabled)return!1;return this.controls.length>0||this.disabled}_registerControl(n){n.setParent(this),n._registerOnCollectionChange(this._onCollectionChange)}_find(n){return this.at(n)??null}};var k2=(()=>{class t extends Di{callSetDisabledState;get submitted(){return Ne(this._submittedReactive)}set submitted(e){this._submittedReactive.set(e)}_submitted=xt(()=>this._submittedReactive());_submittedReactive=M(!1);_oldForm;_onCollectionChange=()=>this._updateDomValue();directives=[];constructor(e,i,r){super(),this.callSetDisabledState=r,this._setValidators(e),this._setAsyncValidators(i)}ngOnChanges(e){this.onChanges(e)}ngOnDestroy(){this.onDestroy()}onChanges(e){this._checkFormPresent(),e.hasOwnProperty("form")&&(this._updateValidators(),this._updateDomValue(),this._updateRegistrations(),this._oldForm=this.form)}onDestroy(){this.form&&(Xf(this.form,this),this.form._onCollectionChange===this._onCollectionChange&&this.form._registerOnCollectionChange(()=>{}))}get formDirective(){return this}get path(){return[]}addControl(e){let i=this.form.get(e.path);return od(i,e,this.callSetDisabledState),i.updateValueAndValidity({emitEvent:!1}),this.directives.push(e),i}getControl(e){return this.form.get(e.path)}removeControl(e){Zf(e.control||null,e,!1),w2(this.directives,e)}addFormGroup(e){this._setUpFormContainer(e)}removeFormGroup(e){this._cleanUpFormContainer(e)}getFormGroup(e){return this.form.get(e.path)}getFormArray(e){return this.form.get(e.path)}addFormArray(e){this._setUpFormContainer(e)}removeFormArray(e){this._cleanUpFormContainer(e)}updateModel(e,i){this.form.get(e.path).setValue(i)}onReset(){this.resetForm()}resetForm(e=void 0,i={}){this.form.reset(e,i),this._submittedReactive.set(!1)}onSubmit(e){return this.submitted=!0,MI(this.form,this.directives),this.ngSubmit.emit(e),this.form._events.next(new Yf(this.control)),e?.target?.method==="dialog"}_updateDomValue(){this.directives.forEach(e=>{let i=e.control,r=this.form.get(e.path);i!==r&&(Zf(i||null,e),E2(r)&&(od(r,e,this.callSetDisabledState),e.control=r))}),this.form._updateTreeValidity({emitEvent:!1})}_setUpFormContainer(e){let i=this.form.get(e.path);kI(i,e),i.updateValueAndValidity({emitEvent:!1})}_cleanUpFormContainer(e){let i=this.form?.get(e.path);i&&x2(i,e)&&i.updateValueAndValidity({emitEvent:!1})}_updateRegistrations(){this.form._registerOnCollectionChange(this._onCollectionChange),this._oldForm?._registerOnCollectionChange(()=>{})}_updateValidators(){Zb(this.form,this),this._oldForm&&Xf(this._oldForm,this)}_checkFormPresent(){this.form}static \u0275fac=function(i){return new(i||t)(ee(Si,10),ee(sd,10),ee(Va,8))};static \u0275dir=A({type:t,features:[fe,ze]})}return t})();var Jb=new y(""),M2={provide:Cn,useExisting:It(()=>ey)},ey=(()=>{class t extends Cn{_ngModelWarningConfig;callSetDisabledState;viewModel;form;set isDisabled(e){}model;update=new U;static _ngModelWarningSentOnce=!1;_ngModelWarningSent=!1;constructor(e,i,r,o,s){super(),this._ngModelWarningConfig=o,this.callSetDisabledState=s,this._setValidators(e),this._setAsyncValidators(i),this.valueAccessor=Xb(this,r)}ngOnChanges(e){if(this._isControlChanged(e)){let i=e.form.previousValue;i&&Zf(i,this,!1),od(this.form,this,this.callSetDisabledState),this.form.updateValueAndValidity({emitEvent:!1})}Qb(e,this.viewModel)&&(this.form.setValue(this.model),this.viewModel=this.model)}ngOnDestroy(){this.form&&Zf(this.form,this,!1)}get path(){return[]}get control(){return this.form}viewToModelUpdate(e){this.viewModel=e,this.update.emit(e)}_isControlChanged(e){return e.hasOwnProperty("form")}static \u0275fac=function(i){return new(i||t)(ee(Si,10),ee(sd,10),ee(Ei,10),ee(Jb,8),ee(Va,8))};static \u0275dir=A({type:t,selectors:[["","formControl",""]],inputs:{form:[0,"formControl","form"],isDisabled:[0,"disabled","isDisabled"],model:[0,"ngModel","model"]},outputs:{update:"ngModelChange"},exportAs:["ngForm"],standalone:!1,features:[Ee([M2]),fe,ze]})}return t})();var T2={provide:Cn,useExisting:It(()=>ad)},ad=(()=>{class t extends Cn{_ngModelWarningConfig;_added=!1;viewModel;control;name=null;set isDisabled(e){}model;update=new U;static _ngModelWarningSentOnce=!1;_ngModelWarningSent=!1;constructor(e,i,r,o,s){super(),this._ngModelWarningConfig=s,this._parent=e,this._setValidators(i),this._setAsyncValidators(r),this.valueAccessor=Xb(this,o)}ngOnChanges(e){this._added||this._setUpControl(),Qb(e,this.viewModel)&&(this.viewModel=this.model,this.formDirective.updateModel(this,this.model))}ngOnDestroy(){this.formDirective?.removeControl(this)}viewToModelUpdate(e){this.viewModel=e,this.update.emit(e)}get path(){return SI(this.name==null?this.name:this.name.toString(),this._parent)}get formDirective(){return this._parent?this._parent.formDirective:null}_setUpControl(){this.control=this.formDirective.addControl(this),this._added=!0}static \u0275fac=function(i){return new(i||t)(ee(Di,13),ee(Si,10),ee(sd,10),ee(Ei,10),ee(Jb,8))};static \u0275dir=A({type:t,selectors:[["","formControlName",""]],inputs:{name:[0,"formControlName","name"],isDisabled:[0,"disabled","isDisabled"],model:[0,"ngModel","model"]},outputs:{update:"ngModelChange"},standalone:!1,features:[Ee([T2]),fe,ze]})}return t})();var A2={provide:Di,useExisting:It(()=>Dr)},Dr=(()=>{class t extends k2{form=null;ngSubmit=new U;get control(){return this.form}static \u0275fac=(()=>{let e;return function(r){return(e||(e=ct(t)))(r||t)}})();static \u0275dir=A({type:t,selectors:[["","formGroup",""]],hostBindings:function(i,r){i&1&&T("submit",function(s){return r.onSubmit(s)})("reset",function(){return r.onReset()})},inputs:{form:[0,"formGroup","form"]},outputs:{ngSubmit:"ngSubmit"},exportAs:["ngForm"],standalone:!1,features:[Ee([A2]),fe]})}return t})();function R2(t){return typeof t=="number"?t:parseFloat(t)}var TI=(()=>{class t{_validator=zf;_onChange;_enabled;ngOnChanges(e){if(this.inputName in e){let i=this.normalizeInput(e[this.inputName].currentValue);this._enabled=this.enabled(i),this._validator=this._enabled?this.createValidator(i):zf,this._onChange?.()}}validate(e){return this._validator(e)}registerOnValidatorChange(e){this._onChange=e}enabled(e){return e!=null}static \u0275fac=function(i){return new(i||t)};static \u0275dir=A({type:t,features:[ze]})}return t})();var O2={provide:Si,useExisting:It(()=>ty),multi:!0},ty=(()=>{class t extends TI{min;inputName="min";normalizeInput=e=>R2(e);createValidator=e=>fI(e);static \u0275fac=(()=>{let e;return function(r){return(e||(e=ct(t)))(r||t)}})();static \u0275dir=A({type:t,selectors:[["input","type","number","min","","formControlName",""],["input","type","number","min","","formControl",""],["input","type","number","min","","ngModel",""]],hostVars:1,hostBindings:function(i,r){i&2&&q("min",r._enabled?r.min:null)},inputs:{min:"min"},standalone:!1,features:[Ee([O2]),fe]})}return t})(),N2={provide:Si,useExisting:It(()=>ny),multi:!0};var ny=(()=>{class t extends TI{required;inputName="required";normalizeInput=B;createValidator=e=>hI;enabled(e){return e}static \u0275fac=(()=>{let e;return function(r){return(e||(e=ct(t)))(r||t)}})();static \u0275dir=A({type:t,selectors:[["","required","","formControlName","",3,"type","checkbox"],["","required","","formControl","",3,"type","checkbox"],["","required","","ngModel","",3,"type","checkbox"]],hostVars:1,hostBindings:function(i,r){i&2&&q("required",r._enabled?"":null)},inputs:{required:"required"},standalone:!1,features:[Ee([N2]),fe]})}return t})();var AI=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=ae({type:t});static \u0275inj=oe({})}return t})();function dI(t){return!!t&&(t.asyncValidators!==void 0||t.validators!==void 0||t.updateOn!==void 0)}var eh=(()=>{class t{useNonNullable=!1;get nonNullable(){let e=new t;return e.useNonNullable=!0,e}group(e,i=null){let r=this._reduceControls(e),o={};return dI(i)?o=i:i!==null&&(o.validators=i.validator,o.asyncValidators=i.asyncValidator),new ja(r,o)}record(e,i=null){let r=this._reduceControls(e);return new Hb(r,i)}control(e,i,r){let o={};return this.useNonNullable?(dI(i)?o=i:(o.validators=i,o.asyncValidators=r),new id(e,Z(b({},o),{nonNullable:!0}))):new id(e,i,r)}array(e,i,r){let o=e.map(s=>this._createControl(s));return new Ub(o,i,r)}_reduceControls(e){let i={};return Object.keys(e).forEach(r=>{i[r]=this._createControl(e[r])}),i}_createControl(e){if(e instanceof id)return e;if(e instanceof La)return e;if(Array.isArray(e)){let i=e[0],r=e.length>1?e[1]:null,o=e.length>2?e[2]:null;return this.control(i,r,o)}else return this.control(e)}static \u0275fac=function(i){return new(i||t)};static \u0275prov=C({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();var Ua=(()=>{class t{static withConfig(e){return{ngModule:t,providers:[{provide:Va,useValue:e.callSetDisabledState??Jf}]}}static \u0275fac=function(i){return new(i||t)};static \u0275mod=ae({type:t});static \u0275inj=oe({imports:[AI]})}return t})(),za=(()=>{class t{static withConfig(e){return{ngModule:t,providers:[{provide:Jb,useValue:e.warnOnNgModelWithFormControl??"always"},{provide:Va,useValue:e.callSetDisabledState??Jf}]}}static \u0275fac=function(i){return new(i||t)};static \u0275mod=ae({type:t});static \u0275inj=oe({imports:[AI]})}return t})();var Cs=(()=>{class t{get vertical(){return this._vertical}set vertical(e){this._vertical=Rt(e)}_vertical=!1;get inset(){return this._inset}set inset(e){this._inset=Rt(e)}_inset=!1;static \u0275fac=function(i){return new(i||t)};static \u0275cmp=E({type:t,selectors:[["mat-divider"]],hostAttrs:["role","separator",1,"mat-divider"],hostVars:7,hostBindings:function(i,r){i&2&&(q("aria-orientation",r.vertical?"vertical":"horizontal"),z("mat-divider-vertical",r.vertical)("mat-divider-horizontal",!r.vertical)("mat-divider-inset",r.inset))},inputs:{vertical:"vertical",inset:"inset"},decls:0,vars:0,template:function(i,r){},styles:[`.mat-divider {
  display: block;
  margin: 0;
  border-top-style: solid;
  border-top-color: var(--mat-divider-color, var(--mat-sys-outline-variant));
  border-top-width: var(--mat-divider-width, 1px);
}
.mat-divider.mat-divider-vertical {
  border-top: 0;
  border-right-style: solid;
  border-right-color: var(--mat-divider-color, var(--mat-sys-outline-variant));
  border-right-width: var(--mat-divider-width, 1px);
}
.mat-divider.mat-divider-inset {
  margin-left: 80px;
}
[dir=rtl] .mat-divider.mat-divider-inset {
  margin-left: auto;
  margin-right: 80px;
}
`],encapsulation:2,changeDetection:0})}return t})();var RI=(()=>{class t{_animationsDisabled=ke();state="unchecked";disabled=!1;appearance="full";constructor(){}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=E({type:t,selectors:[["mat-pseudo-checkbox"]],hostAttrs:[1,"mat-pseudo-checkbox"],hostVars:12,hostBindings:function(i,r){i&2&&z("mat-pseudo-checkbox-indeterminate",r.state==="indeterminate")("mat-pseudo-checkbox-checked",r.state==="checked")("mat-pseudo-checkbox-disabled",r.disabled)("mat-pseudo-checkbox-minimal",r.appearance==="minimal")("mat-pseudo-checkbox-full",r.appearance==="full")("_mat-animation-noopable",r._animationsDisabled)},inputs:{state:"state",disabled:"disabled",appearance:"appearance"},decls:0,vars:0,template:function(i,r){},styles:[`.mat-pseudo-checkbox {
  border-radius: 2px;
  cursor: pointer;
  display: inline-block;
  vertical-align: middle;
  box-sizing: border-box;
  position: relative;
  flex-shrink: 0;
  transition: border-color 90ms cubic-bezier(0, 0, 0.2, 0.1), background-color 90ms cubic-bezier(0, 0, 0.2, 0.1);
}
.mat-pseudo-checkbox::after {
  position: absolute;
  opacity: 0;
  content: "";
  border-bottom: 2px solid currentColor;
  transition: opacity 90ms cubic-bezier(0, 0, 0.2, 0.1);
}
.mat-pseudo-checkbox._mat-animation-noopable {
  transition: none !important;
  animation: none !important;
}
.mat-pseudo-checkbox._mat-animation-noopable::after {
  transition: none;
}

.mat-pseudo-checkbox-disabled {
  cursor: default;
}

.mat-pseudo-checkbox-indeterminate::after {
  left: 1px;
  opacity: 1;
  border-radius: 2px;
}

.mat-pseudo-checkbox-checked::after {
  left: 1px;
  border-left: 2px solid currentColor;
  transform: rotate(-45deg);
  opacity: 1;
  box-sizing: content-box;
}

.mat-pseudo-checkbox-minimal.mat-pseudo-checkbox-checked::after, .mat-pseudo-checkbox-minimal.mat-pseudo-checkbox-indeterminate::after {
  color: var(--mat-pseudo-checkbox-minimal-selected-checkmark-color, var(--mat-sys-primary));
}
.mat-pseudo-checkbox-minimal.mat-pseudo-checkbox-checked.mat-pseudo-checkbox-disabled::after, .mat-pseudo-checkbox-minimal.mat-pseudo-checkbox-indeterminate.mat-pseudo-checkbox-disabled::after {
  color: var(--mat-pseudo-checkbox-minimal-disabled-selected-checkmark-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
}

.mat-pseudo-checkbox-full {
  border-color: var(--mat-pseudo-checkbox-full-unselected-icon-color, var(--mat-sys-on-surface-variant));
  border-width: 2px;
  border-style: solid;
}
.mat-pseudo-checkbox-full.mat-pseudo-checkbox-disabled {
  border-color: var(--mat-pseudo-checkbox-full-disabled-unselected-icon-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
}
.mat-pseudo-checkbox-full.mat-pseudo-checkbox-checked, .mat-pseudo-checkbox-full.mat-pseudo-checkbox-indeterminate {
  background-color: var(--mat-pseudo-checkbox-full-selected-icon-color, var(--mat-sys-primary));
  border-color: transparent;
}
.mat-pseudo-checkbox-full.mat-pseudo-checkbox-checked::after, .mat-pseudo-checkbox-full.mat-pseudo-checkbox-indeterminate::after {
  color: var(--mat-pseudo-checkbox-full-selected-checkmark-color, var(--mat-sys-on-primary));
}
.mat-pseudo-checkbox-full.mat-pseudo-checkbox-checked.mat-pseudo-checkbox-disabled, .mat-pseudo-checkbox-full.mat-pseudo-checkbox-indeterminate.mat-pseudo-checkbox-disabled {
  background-color: var(--mat-pseudo-checkbox-full-disabled-selected-icon-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
}
.mat-pseudo-checkbox-full.mat-pseudo-checkbox-checked.mat-pseudo-checkbox-disabled::after, .mat-pseudo-checkbox-full.mat-pseudo-checkbox-indeterminate.mat-pseudo-checkbox-disabled::after {
  color: var(--mat-pseudo-checkbox-full-disabled-selected-checkmark-color, var(--mat-sys-surface));
}

.mat-pseudo-checkbox {
  width: 18px;
  height: 18px;
}

.mat-pseudo-checkbox-minimal.mat-pseudo-checkbox-checked::after {
  width: 14px;
  height: 6px;
  transform-origin: center;
  top: -4.2426406871px;
  left: 0;
  bottom: 0;
  right: 0;
  margin: auto;
}
.mat-pseudo-checkbox-minimal.mat-pseudo-checkbox-indeterminate::after {
  top: 8px;
  width: 16px;
}

.mat-pseudo-checkbox-full.mat-pseudo-checkbox-checked::after {
  width: 10px;
  height: 4px;
  transform-origin: center;
  top: -2.8284271247px;
  left: 0;
  bottom: 0;
  right: 0;
  margin: auto;
}
.mat-pseudo-checkbox-full.mat-pseudo-checkbox-indeterminate::after {
  top: 6px;
  width: 12px;
}
`],encapsulation:2,changeDetection:0})}return t})();var P2=["*"],F2=`.mdc-list {
  margin: 0;
  padding: 8px 0;
  list-style-type: none;
}
.mdc-list:focus {
  outline: none;
}

.mdc-list-item {
  display: flex;
  position: relative;
  justify-content: flex-start;
  overflow: hidden;
  padding: 0;
  align-items: stretch;
  cursor: pointer;
  padding-left: 16px;
  padding-right: 16px;
  background-color: var(--mat-list-list-item-container-color, transparent);
  border-radius: var(--mat-list-list-item-container-shape, var(--mat-sys-corner-none));
}
.mdc-list-item.mdc-list-item--selected {
  background-color: var(--mat-list-list-item-selected-container-color);
}
.mdc-list-item:focus {
  outline: 0;
}
.mdc-list-item.mdc-list-item--disabled {
  cursor: auto;
}
.mdc-list-item.mdc-list-item--with-one-line {
  height: var(--mat-list-list-item-one-line-container-height, 48px);
}
.mdc-list-item.mdc-list-item--with-one-line .mdc-list-item__start {
  align-self: center;
  margin-top: 0;
}
.mdc-list-item.mdc-list-item--with-one-line .mdc-list-item__end {
  align-self: center;
  margin-top: 0;
}
.mdc-list-item.mdc-list-item--with-two-lines {
  height: var(--mat-list-list-item-two-line-container-height, 64px);
}
.mdc-list-item.mdc-list-item--with-two-lines .mdc-list-item__start {
  align-self: flex-start;
  margin-top: 16px;
}
.mdc-list-item.mdc-list-item--with-two-lines .mdc-list-item__end {
  align-self: center;
  margin-top: 0;
}
.mdc-list-item.mdc-list-item--with-three-lines {
  height: var(--mat-list-list-item-three-line-container-height, 88px);
}
.mdc-list-item.mdc-list-item--with-three-lines .mdc-list-item__start {
  align-self: flex-start;
  margin-top: 16px;
}
.mdc-list-item.mdc-list-item--with-three-lines .mdc-list-item__end {
  align-self: flex-start;
  margin-top: 16px;
}
.mdc-list-item.mdc-list-item--selected::before, .mdc-list-item.mdc-list-item--selected:focus::before, .mdc-list-item:not(.mdc-list-item--selected):focus::before {
  position: absolute;
  box-sizing: border-box;
  width: 100%;
  height: 100%;
  top: 0;
  left: 0;
  content: "";
  pointer-events: none;
}

a.mdc-list-item {
  color: inherit;
  text-decoration: none;
}

.mdc-list-item__start {
  fill: currentColor;
  flex-shrink: 0;
  pointer-events: none;
}
.mdc-list-item--with-leading-icon .mdc-list-item__start {
  color: var(--mat-list-list-item-leading-icon-color, var(--mat-sys-on-surface-variant));
  width: var(--mat-list-list-item-leading-icon-size, 24px);
  height: var(--mat-list-list-item-leading-icon-size, 24px);
  margin-left: 16px;
  margin-right: 32px;
}
[dir=rtl] .mdc-list-item--with-leading-icon .mdc-list-item__start {
  margin-left: 32px;
  margin-right: 16px;
}
.mdc-list-item--with-leading-icon:hover .mdc-list-item__start {
  color: var(--mat-list-list-item-hover-leading-icon-color);
}
.mdc-list-item--with-leading-avatar .mdc-list-item__start {
  width: var(--mat-list-list-item-leading-avatar-size, 40px);
  height: var(--mat-list-list-item-leading-avatar-size, 40px);
  margin-left: 16px;
  margin-right: 16px;
  border-radius: 50%;
}
.mdc-list-item--with-leading-avatar .mdc-list-item__start, [dir=rtl] .mdc-list-item--with-leading-avatar .mdc-list-item__start {
  margin-left: 16px;
  margin-right: 16px;
  border-radius: 50%;
}

.mdc-list-item__end {
  flex-shrink: 0;
  pointer-events: none;
}
.mdc-list-item--with-trailing-meta .mdc-list-item__end {
  font-family: var(--mat-list-list-item-trailing-supporting-text-font, var(--mat-sys-label-small-font));
  line-height: var(--mat-list-list-item-trailing-supporting-text-line-height, var(--mat-sys-label-small-line-height));
  font-size: var(--mat-list-list-item-trailing-supporting-text-size, var(--mat-sys-label-small-size));
  font-weight: var(--mat-list-list-item-trailing-supporting-text-weight, var(--mat-sys-label-small-weight));
  letter-spacing: var(--mat-list-list-item-trailing-supporting-text-tracking, var(--mat-sys-label-small-tracking));
}
.mdc-list-item--with-trailing-icon .mdc-list-item__end {
  color: var(--mat-list-list-item-trailing-icon-color, var(--mat-sys-on-surface-variant));
  width: var(--mat-list-list-item-trailing-icon-size, 24px);
  height: var(--mat-list-list-item-trailing-icon-size, 24px);
}
.mdc-list-item--with-trailing-icon:hover .mdc-list-item__end {
  color: var(--mat-list-list-item-hover-trailing-icon-color);
}
.mdc-list-item.mdc-list-item--with-trailing-meta .mdc-list-item__end {
  color: var(--mat-list-list-item-trailing-supporting-text-color, var(--mat-sys-on-surface-variant));
}
.mdc-list-item--selected.mdc-list-item--with-trailing-icon .mdc-list-item__end {
  color: var(--mat-list-list-item-selected-trailing-icon-color, var(--mat-sys-primary));
}

.mdc-list-item__content {
  text-overflow: ellipsis;
  white-space: nowrap;
  overflow: hidden;
  align-self: center;
  flex: 1;
  pointer-events: none;
}
.mdc-list-item--with-two-lines .mdc-list-item__content, .mdc-list-item--with-three-lines .mdc-list-item__content {
  align-self: stretch;
}

.mdc-list-item__primary-text {
  text-overflow: ellipsis;
  white-space: nowrap;
  overflow: hidden;
  color: var(--mat-list-list-item-label-text-color, var(--mat-sys-on-surface));
  font-family: var(--mat-list-list-item-label-text-font, var(--mat-sys-body-large-font));
  line-height: var(--mat-list-list-item-label-text-line-height, var(--mat-sys-body-large-line-height));
  font-size: var(--mat-list-list-item-label-text-size, var(--mat-sys-body-large-size));
  font-weight: var(--mat-list-list-item-label-text-weight, var(--mat-sys-body-large-weight));
  letter-spacing: var(--mat-list-list-item-label-text-tracking, var(--mat-sys-body-large-tracking));
}
.mdc-list-item:hover .mdc-list-item__primary-text {
  color: var(--mat-list-list-item-hover-label-text-color, var(--mat-sys-on-surface));
}
.mdc-list-item:focus .mdc-list-item__primary-text {
  color: var(--mat-list-list-item-focus-label-text-color, var(--mat-sys-on-surface));
}
.mdc-list-item--with-two-lines .mdc-list-item__primary-text, .mdc-list-item--with-three-lines .mdc-list-item__primary-text {
  display: block;
  margin-top: 0;
  line-height: normal;
  margin-bottom: -20px;
}
.mdc-list-item--with-two-lines .mdc-list-item__primary-text::before, .mdc-list-item--with-three-lines .mdc-list-item__primary-text::before {
  display: inline-block;
  width: 0;
  height: 28px;
  content: "";
  vertical-align: 0;
}
.mdc-list-item--with-two-lines .mdc-list-item__primary-text::after, .mdc-list-item--with-three-lines .mdc-list-item__primary-text::after {
  display: inline-block;
  width: 0;
  height: 20px;
  content: "";
  vertical-align: -20px;
}

.mdc-list-item__secondary-text {
  text-overflow: ellipsis;
  white-space: nowrap;
  overflow: hidden;
  display: block;
  margin-top: 0;
  color: var(--mat-list-list-item-supporting-text-color, var(--mat-sys-on-surface-variant));
  font-family: var(--mat-list-list-item-supporting-text-font, var(--mat-sys-body-medium-font));
  line-height: var(--mat-list-list-item-supporting-text-line-height, var(--mat-sys-body-medium-line-height));
  font-size: var(--mat-list-list-item-supporting-text-size, var(--mat-sys-body-medium-size));
  font-weight: var(--mat-list-list-item-supporting-text-weight, var(--mat-sys-body-medium-weight));
  letter-spacing: var(--mat-list-list-item-supporting-text-tracking, var(--mat-sys-body-medium-tracking));
}
.mdc-list-item__secondary-text::before {
  display: inline-block;
  width: 0;
  height: 20px;
  content: "";
  vertical-align: 0;
}
.mdc-list-item--with-three-lines .mdc-list-item__secondary-text {
  white-space: normal;
  line-height: 20px;
}
.mdc-list-item--with-overline .mdc-list-item__secondary-text {
  white-space: nowrap;
  line-height: auto;
}

.mdc-list-item--with-leading-radio.mdc-list-item,
.mdc-list-item--with-leading-checkbox.mdc-list-item,
.mdc-list-item--with-leading-icon.mdc-list-item,
.mdc-list-item--with-leading-avatar.mdc-list-item {
  padding-left: 0;
  padding-right: 16px;
}
[dir=rtl] .mdc-list-item--with-leading-radio.mdc-list-item,
[dir=rtl] .mdc-list-item--with-leading-checkbox.mdc-list-item,
[dir=rtl] .mdc-list-item--with-leading-icon.mdc-list-item,
[dir=rtl] .mdc-list-item--with-leading-avatar.mdc-list-item {
  padding-left: 16px;
  padding-right: 0;
}
.mdc-list-item--with-leading-radio.mdc-list-item--with-two-lines .mdc-list-item__primary-text,
.mdc-list-item--with-leading-checkbox.mdc-list-item--with-two-lines .mdc-list-item__primary-text,
.mdc-list-item--with-leading-icon.mdc-list-item--with-two-lines .mdc-list-item__primary-text,
.mdc-list-item--with-leading-avatar.mdc-list-item--with-two-lines .mdc-list-item__primary-text {
  display: block;
  margin-top: 0;
  line-height: normal;
  margin-bottom: -20px;
}
.mdc-list-item--with-leading-radio.mdc-list-item--with-two-lines .mdc-list-item__primary-text::before,
.mdc-list-item--with-leading-checkbox.mdc-list-item--with-two-lines .mdc-list-item__primary-text::before,
.mdc-list-item--with-leading-icon.mdc-list-item--with-two-lines .mdc-list-item__primary-text::before,
.mdc-list-item--with-leading-avatar.mdc-list-item--with-two-lines .mdc-list-item__primary-text::before {
  display: inline-block;
  width: 0;
  height: 32px;
  content: "";
  vertical-align: 0;
}
.mdc-list-item--with-leading-radio.mdc-list-item--with-two-lines .mdc-list-item__primary-text::after,
.mdc-list-item--with-leading-checkbox.mdc-list-item--with-two-lines .mdc-list-item__primary-text::after,
.mdc-list-item--with-leading-icon.mdc-list-item--with-two-lines .mdc-list-item__primary-text::after,
.mdc-list-item--with-leading-avatar.mdc-list-item--with-two-lines .mdc-list-item__primary-text::after {
  display: inline-block;
  width: 0;
  height: 20px;
  content: "";
  vertical-align: -20px;
}
.mdc-list-item--with-leading-radio.mdc-list-item--with-two-lines.mdc-list-item--with-trailing-meta .mdc-list-item__end,
.mdc-list-item--with-leading-checkbox.mdc-list-item--with-two-lines.mdc-list-item--with-trailing-meta .mdc-list-item__end,
.mdc-list-item--with-leading-icon.mdc-list-item--with-two-lines.mdc-list-item--with-trailing-meta .mdc-list-item__end,
.mdc-list-item--with-leading-avatar.mdc-list-item--with-two-lines.mdc-list-item--with-trailing-meta .mdc-list-item__end {
  display: block;
  margin-top: 0;
  line-height: normal;
}
.mdc-list-item--with-leading-radio.mdc-list-item--with-two-lines.mdc-list-item--with-trailing-meta .mdc-list-item__end::before,
.mdc-list-item--with-leading-checkbox.mdc-list-item--with-two-lines.mdc-list-item--with-trailing-meta .mdc-list-item__end::before,
.mdc-list-item--with-leading-icon.mdc-list-item--with-two-lines.mdc-list-item--with-trailing-meta .mdc-list-item__end::before,
.mdc-list-item--with-leading-avatar.mdc-list-item--with-two-lines.mdc-list-item--with-trailing-meta .mdc-list-item__end::before {
  display: inline-block;
  width: 0;
  height: 32px;
  content: "";
  vertical-align: 0;
}

.mdc-list-item--with-trailing-icon.mdc-list-item, [dir=rtl] .mdc-list-item--with-trailing-icon.mdc-list-item {
  padding-left: 0;
  padding-right: 0;
}
.mdc-list-item--with-trailing-icon .mdc-list-item__end {
  margin-left: 16px;
  margin-right: 16px;
}

.mdc-list-item--with-trailing-meta.mdc-list-item {
  padding-left: 16px;
  padding-right: 0;
}
[dir=rtl] .mdc-list-item--with-trailing-meta.mdc-list-item {
  padding-left: 0;
  padding-right: 16px;
}
.mdc-list-item--with-trailing-meta .mdc-list-item__end {
  -webkit-user-select: none;
  user-select: none;
  margin-left: 28px;
  margin-right: 16px;
}
[dir=rtl] .mdc-list-item--with-trailing-meta .mdc-list-item__end {
  margin-left: 16px;
  margin-right: 28px;
}
.mdc-list-item--with-trailing-meta.mdc-list-item--with-three-lines .mdc-list-item__end, .mdc-list-item--with-trailing-meta.mdc-list-item--with-two-lines .mdc-list-item__end {
  display: block;
  line-height: normal;
  align-self: flex-start;
  margin-top: 0;
}
.mdc-list-item--with-trailing-meta.mdc-list-item--with-three-lines .mdc-list-item__end::before, .mdc-list-item--with-trailing-meta.mdc-list-item--with-two-lines .mdc-list-item__end::before {
  display: inline-block;
  width: 0;
  height: 28px;
  content: "";
  vertical-align: 0;
}

.mdc-list-item--with-leading-radio .mdc-list-item__start,
.mdc-list-item--with-leading-checkbox .mdc-list-item__start {
  margin-left: 8px;
  margin-right: 24px;
}
[dir=rtl] .mdc-list-item--with-leading-radio .mdc-list-item__start,
[dir=rtl] .mdc-list-item--with-leading-checkbox .mdc-list-item__start {
  margin-left: 24px;
  margin-right: 8px;
}
.mdc-list-item--with-leading-radio.mdc-list-item--with-two-lines .mdc-list-item__start,
.mdc-list-item--with-leading-checkbox.mdc-list-item--with-two-lines .mdc-list-item__start {
  align-self: flex-start;
  margin-top: 8px;
}

.mdc-list-item--with-trailing-radio.mdc-list-item,
.mdc-list-item--with-trailing-checkbox.mdc-list-item {
  padding-left: 16px;
  padding-right: 0;
}
[dir=rtl] .mdc-list-item--with-trailing-radio.mdc-list-item,
[dir=rtl] .mdc-list-item--with-trailing-checkbox.mdc-list-item {
  padding-left: 0;
  padding-right: 16px;
}
.mdc-list-item--with-trailing-radio.mdc-list-item--with-leading-icon, .mdc-list-item--with-trailing-radio.mdc-list-item--with-leading-avatar,
.mdc-list-item--with-trailing-checkbox.mdc-list-item--with-leading-icon,
.mdc-list-item--with-trailing-checkbox.mdc-list-item--with-leading-avatar {
  padding-left: 0;
}
[dir=rtl] .mdc-list-item--with-trailing-radio.mdc-list-item--with-leading-icon, [dir=rtl] .mdc-list-item--with-trailing-radio.mdc-list-item--with-leading-avatar,
[dir=rtl] .mdc-list-item--with-trailing-checkbox.mdc-list-item--with-leading-icon,
[dir=rtl] .mdc-list-item--with-trailing-checkbox.mdc-list-item--with-leading-avatar {
  padding-right: 0;
}
.mdc-list-item--with-trailing-radio .mdc-list-item__end,
.mdc-list-item--with-trailing-checkbox .mdc-list-item__end {
  margin-left: 24px;
  margin-right: 8px;
}
[dir=rtl] .mdc-list-item--with-trailing-radio .mdc-list-item__end,
[dir=rtl] .mdc-list-item--with-trailing-checkbox .mdc-list-item__end {
  margin-left: 8px;
  margin-right: 24px;
}
.mdc-list-item--with-trailing-radio.mdc-list-item--with-three-lines .mdc-list-item__end,
.mdc-list-item--with-trailing-checkbox.mdc-list-item--with-three-lines .mdc-list-item__end {
  align-self: flex-start;
  margin-top: 8px;
}

.mdc-list-group__subheader {
  margin: 0.75rem 16px;
}

.mdc-list-item--disabled .mdc-list-item__start,
.mdc-list-item--disabled .mdc-list-item__content,
.mdc-list-item--disabled .mdc-list-item__end {
  opacity: 1;
}
.mdc-list-item--disabled .mdc-list-item__primary-text,
.mdc-list-item--disabled .mdc-list-item__secondary-text {
  opacity: var(--mat-list-list-item-disabled-label-text-opacity, 0.3);
}
.mdc-list-item--disabled.mdc-list-item--with-leading-icon .mdc-list-item__start {
  color: var(--mat-list-list-item-disabled-leading-icon-color, var(--mat-sys-on-surface));
  opacity: var(--mat-list-list-item-disabled-leading-icon-opacity, 0.38);
}
.mdc-list-item--disabled.mdc-list-item--with-trailing-icon .mdc-list-item__end {
  color: var(--mat-list-list-item-disabled-trailing-icon-color, var(--mat-sys-on-surface));
  opacity: var(--mat-list-list-item-disabled-trailing-icon-opacity, 0.38);
}

.mat-mdc-list-item.mat-mdc-list-item-both-leading-and-trailing, [dir=rtl] .mat-mdc-list-item.mat-mdc-list-item-both-leading-and-trailing {
  padding-left: 0;
  padding-right: 0;
}

.mdc-list-item.mdc-list-item--disabled .mdc-list-item__primary-text {
  color: var(--mat-list-list-item-disabled-label-text-color, var(--mat-sys-on-surface));
}

.mdc-list-item:hover::before {
  background-color: var(--mat-list-list-item-hover-state-layer-color, var(--mat-sys-on-surface));
  opacity: var(--mat-list-list-item-hover-state-layer-opacity, var(--mat-sys-hover-state-layer-opacity));
}

.mdc-list-item.mdc-list-item--disabled::before {
  background-color: var(--mat-list-list-item-disabled-state-layer-color, var(--mat-sys-on-surface));
  opacity: var(--mat-list-list-item-disabled-state-layer-opacity, var(--mat-sys-focus-state-layer-opacity));
}

.mdc-list-item:focus::before {
  background-color: var(--mat-list-list-item-focus-state-layer-color, var(--mat-sys-on-surface));
  opacity: var(--mat-list-list-item-focus-state-layer-opacity, var(--mat-sys-focus-state-layer-opacity));
}

.mdc-list-item--disabled .mdc-radio,
.mdc-list-item--disabled .mdc-checkbox {
  opacity: var(--mat-list-list-item-disabled-label-text-opacity, 0.3);
}

.mdc-list-item--with-leading-avatar .mat-mdc-list-item-avatar {
  border-radius: var(--mat-list-list-item-leading-avatar-shape, var(--mat-sys-corner-full));
  background-color: var(--mat-list-list-item-leading-avatar-color, var(--mat-sys-primary-container));
}

.mat-mdc-list-item-icon {
  font-size: var(--mat-list-list-item-leading-icon-size, 24px);
}

@media (forced-colors: active) {
  a.mdc-list-item--activated::after {
    content: "";
    position: absolute;
    top: 50%;
    right: 16px;
    transform: translateY(-50%);
    width: 10px;
    height: 0;
    border-bottom: solid 10px;
    border-radius: 10px;
  }
  a.mdc-list-item--activated [dir=rtl]::after {
    right: auto;
    left: 16px;
  }
}

.mat-mdc-list-base {
  display: block;
}
.mat-mdc-list-base .mdc-list-item__start,
.mat-mdc-list-base .mdc-list-item__end,
.mat-mdc-list-base .mdc-list-item__content {
  pointer-events: auto;
}

.mat-mdc-list-item,
.mat-mdc-list-option {
  width: 100%;
  box-sizing: border-box;
  -webkit-tap-highlight-color: transparent;
}
.mat-mdc-list-item:not(.mat-mdc-list-item-interactive),
.mat-mdc-list-option:not(.mat-mdc-list-item-interactive) {
  cursor: default;
}
.mat-mdc-list-item .mat-divider-inset,
.mat-mdc-list-option .mat-divider-inset {
  position: absolute;
  left: 0;
  right: 0;
  bottom: 0;
}
.mat-mdc-list-item .mat-mdc-list-item-avatar ~ .mat-divider-inset,
.mat-mdc-list-option .mat-mdc-list-item-avatar ~ .mat-divider-inset {
  margin-left: 72px;
}
[dir=rtl] .mat-mdc-list-item .mat-mdc-list-item-avatar ~ .mat-divider-inset,
[dir=rtl] .mat-mdc-list-option .mat-mdc-list-item-avatar ~ .mat-divider-inset {
  margin-right: 72px;
}

.mat-mdc-list-item-interactive::before {
  top: 0;
  left: 0;
  right: 0;
  bottom: 0;
  position: absolute;
  content: "";
  opacity: 0;
  pointer-events: none;
  border-radius: inherit;
}

.mat-mdc-list-item > .mat-focus-indicator {
  top: 0;
  left: 0;
  right: 0;
  bottom: 0;
  position: absolute;
  pointer-events: none;
}
.mat-mdc-list-item:focus-visible > .mat-focus-indicator::before {
  content: "";
}

.mat-mdc-list-item.mdc-list-item--with-three-lines .mat-mdc-list-item-line.mdc-list-item__secondary-text {
  white-space: nowrap;
  line-height: normal;
}
.mat-mdc-list-item.mdc-list-item--with-three-lines .mat-mdc-list-item-unscoped-content.mdc-list-item__secondary-text {
  display: -webkit-box;
  -webkit-box-orient: vertical;
  -webkit-line-clamp: 2;
}

mat-action-list button {
  background: none;
  color: inherit;
  border: none;
  font: inherit;
  outline: inherit;
  -webkit-tap-highlight-color: transparent;
  text-align: start;
}
mat-action-list button::-moz-focus-inner {
  border: 0;
}

.mdc-list-item--with-leading-icon .mdc-list-item__start {
  margin-inline-start: var(--mat-list-list-item-leading-icon-start-space, 16px);
  margin-inline-end: var(--mat-list-list-item-leading-icon-end-space, 16px);
}

.mat-mdc-nav-list .mat-mdc-list-item {
  border-radius: var(--mat-list-active-indicator-shape, var(--mat-sys-corner-full));
  --mat-focus-indicator-border-radius: var(--mat-list-active-indicator-shape, var(--mat-sys-corner-full));
}
.mat-mdc-nav-list .mat-mdc-list-item.mdc-list-item--activated {
  background-color: var(--mat-list-active-indicator-color, var(--mat-sys-secondary-container));
}
`,L2=["unscopedContent"];var j2=[[["","matListItemTitle",""]],[["","matListItemLine",""]],"*",[["mat-divider"]],[["","matListItemAvatar",""],["","matListItemIcon",""]]],B2=["[matListItemTitle]","[matListItemLine]","*","mat-divider","[matListItemAvatar],[matListItemIcon]"];function V2(t,n){t&1&&re(0,4)}function H2(t,n){if(t&1&&(f(0,"div",11),O(1,"input",12),f(2,"div",13),Gt(),f(3,"svg",14),O(4,"path",15),h(),Vi(),O(5,"div",16),h()()),t&2){let e=S();z("mdc-checkbox--disabled",e.disabled),p(),D("checked",e.selected)("disabled",e.disabled)}}function U2(t,n){if(t&1&&(f(0,"div",17),O(1,"input",18),f(2,"div",19),O(3,"div",20)(4,"div",21),h()()),t&2){let e=S();z("mdc-radio--disabled",e.disabled),p(),D("checked",e.selected)("disabled",e.disabled)}}function z2(t,n){}function $2(t,n){if(t&1&&(f(0,"span",4),$e(1,z2,0,0,"ng-template",6),h()),t&2){S();let e=Re(3);p(),D("ngTemplateOutlet",e)}}function G2(t,n){}function W2(t,n){if(t&1&&(f(0,"span",5),$e(1,G2,0,0,"ng-template",6),h()),t&2){S();let e=Re(5);p(),D("ngTemplateOutlet",e)}}function q2(t,n){}function Y2(t,n){if(t&1&&$e(0,q2,0,0,"ng-template",6),t&2){S();let e=Re(1);D("ngTemplateOutlet",e)}}function Z2(t,n){}function Q2(t,n){if(t&1&&(f(0,"span",9),$e(1,Z2,0,0,"ng-template",6),h()),t&2){S();let e=Re(3);p(),D("ngTemplateOutlet",e)}}function X2(t,n){}function K2(t,n){if(t&1&&(f(0,"span",9),$e(1,X2,0,0,"ng-template",6),h()),t&2){S();let e=Re(5);p(),D("ngTemplateOutlet",e)}}function J2(t,n){}function ej(t,n){if(t&1&&$e(0,J2,0,0,"ng-template",6),t&2){S();let e=Re(1);D("ngTemplateOutlet",e)}}var NI=new y("ListOption"),tj=(()=>{class t{_elementRef=d(L);constructor(){}static \u0275fac=function(i){return new(i||t)};static \u0275dir=A({type:t,selectors:[["","matListItemTitle",""]],hostAttrs:[1,"mat-mdc-list-item-title","mdc-list-item__primary-text"]})}return t})(),nj=(()=>{class t{_elementRef=d(L);constructor(){}static \u0275fac=function(i){return new(i||t)};static \u0275dir=A({type:t,selectors:[["","matListItemLine",""]],hostAttrs:[1,"mat-mdc-list-item-line","mdc-list-item__secondary-text"]})}return t})();var PI=(()=>{class t{_listOption=d(NI,{optional:!0});constructor(){}_isAlignedAtStart(){return!this._listOption||this._listOption?._getTogglePosition()==="after"}static \u0275fac=function(i){return new(i||t)};static \u0275dir=A({type:t,hostVars:4,hostBindings:function(i,r){i&2&&z("mdc-list-item__start",r._isAlignedAtStart())("mdc-list-item__end",!r._isAlignedAtStart())}})}return t})(),ij=(()=>{class t extends PI{static \u0275fac=(()=>{let e;return function(r){return(e||(e=ct(t)))(r||t)}})();static \u0275dir=A({type:t,selectors:[["","matListItemAvatar",""]],hostAttrs:[1,"mat-mdc-list-item-avatar"],features:[fe]})}return t})(),rj=(()=>{class t extends PI{static \u0275fac=(()=>{let e;return function(r){return(e||(e=ct(t)))(r||t)}})();static \u0275dir=A({type:t,selectors:[["","matListItemIcon",""]],hostAttrs:[1,"mat-mdc-list-item-icon"],features:[fe]})}return t})(),oj=new y("MAT_LIST_CONFIG"),iy=(()=>{class t{_isNonInteractive=!0;get disableRipple(){return this._disableRipple}set disableRipple(e){this._disableRipple=Rt(e)}_disableRipple=!1;get disabled(){return this._disabled()}set disabled(e){this._disabled.set(Rt(e))}_disabled=M(!1);_defaultOptions=d(oj,{optional:!0});static \u0275fac=function(i){return new(i||t)};static \u0275dir=A({type:t,hostVars:1,hostBindings:function(i,r){i&2&&q("aria-disabled",r.disabled)},inputs:{disableRipple:"disableRipple",disabled:"disabled"}})}return t})(),OI=(()=>{class t{_elementRef=d(L);_ngZone=d(H);_listBase=d(iy,{optional:!0});_platform=d(ye);_hostElement;_isButtonElement;_noopAnimations=ke();_avatars;_icons;set lines(e){this._explicitLines=Hn(e,null),this._updateItemLines(!1)}_explicitLines=null;get disableRipple(){return this.disabled||this._disableRipple||this._noopAnimations||!!this._listBase?.disableRipple}set disableRipple(e){this._disableRipple=Rt(e)}_disableRipple=!1;get disabled(){return this._disabled()||!!this._listBase?.disabled}set disabled(e){this._disabled.set(Rt(e))}_disabled=M(!1);_subscriptions=new ge;_rippleRenderer=null;_hasUnscopedTextContent=!1;rippleConfig;get rippleDisabled(){return this.disableRipple||!!this.rippleConfig.disabled}constructor(){d(qe).load(An);let e=d(zl,{optional:!0});this.rippleConfig=e||{},this._hostElement=this._elementRef.nativeElement,this._isButtonElement=this._hostElement.nodeName.toLowerCase()==="button",this._listBase&&!this._listBase._isNonInteractive&&this._initInteractiveListItem(),this._isButtonElement&&!this._hostElement.hasAttribute("type")&&this._hostElement.setAttribute("type","button")}ngAfterViewInit(){this._monitorProjectedLinesAndTitle(),this._updateItemLines(!0)}ngOnDestroy(){this._subscriptions.unsubscribe(),this._rippleRenderer!==null&&this._rippleRenderer._removeTriggerEvents()}_hasIconOrAvatar(){return!!(this._avatars.length||this._icons.length)}_initInteractiveListItem(){this._hostElement.classList.add("mat-mdc-list-item-interactive"),this._rippleRenderer=new _s(this,this._ngZone,this._hostElement,this._platform,d(X)),this._rippleRenderer.setupTriggerEvents(this._hostElement)}_monitorProjectedLinesAndTitle(){this._ngZone.runOutsideAngular(()=>{this._subscriptions.add(mn(this._lines.changes,this._titles.changes).subscribe(()=>this._updateItemLines(!1)))})}_updateItemLines(e){if(!this._lines||!this._titles||!this._unscopedContent)return;e&&this._checkDomForUnscopedTextContent();let i=this._explicitLines??this._inferLinesFromContent(),r=this._unscopedContent.nativeElement;if(this._hostElement.classList.toggle("mat-mdc-list-item-single-line",i<=1),this._hostElement.classList.toggle("mdc-list-item--with-one-line",i<=1),this._hostElement.classList.toggle("mdc-list-item--with-two-lines",i===2),this._hostElement.classList.toggle("mdc-list-item--with-three-lines",i===3),this._hasUnscopedTextContent){let o=this._titles.length===0&&i===1;r.classList.toggle("mdc-list-item__primary-text",o),r.classList.toggle("mdc-list-item__secondary-text",!o)}else r.classList.remove("mdc-list-item__primary-text"),r.classList.remove("mdc-list-item__secondary-text")}_inferLinesFromContent(){let e=this._titles.length+this._lines.length;return this._hasUnscopedTextContent&&(e+=1),e}_checkDomForUnscopedTextContent(){this._hasUnscopedTextContent=Array.from(this._unscopedContent.nativeElement.childNodes).filter(e=>e.nodeType!==e.COMMENT_NODE).some(e=>!!(e.textContent&&e.textContent.trim()))}static \u0275fac=function(i){return new(i||t)};static \u0275dir=A({type:t,contentQueries:function(i,r,o){if(i&1&&yt(o,ij,4)(o,rj,4),i&2){let s;$(s=G())&&(r._avatars=s),$(s=G())&&(r._icons=s)}},hostVars:4,hostBindings:function(i,r){i&2&&(q("aria-disabled",r.disabled)("disabled",r._isButtonElement&&r.disabled||null),z("mdc-list-item--disabled",r.disabled))},inputs:{lines:"lines",disableRipple:"disableRipple",disabled:"disabled"}})}return t})();var FI=new y("SelectionList"),cd=(()=>{class t extends OI{_selectionList=d(FI);_changeDetectorRef=d(ve);_lines;_titles;_unscopedContent;selectedChange=new U;togglePosition="after";get checkboxPosition(){return this.togglePosition}set checkboxPosition(e){this.togglePosition=e}get color(){return this._color||this._selectionList.color}set color(e){this._color=e}_color;get value(){return this._value}set value(e){this.selected&&e!==this.value&&this._inputsInitialized&&(this.selected=!1),this._value=e}_value;get selected(){return this._selectionList.selectedOptions.isSelected(this)}set selected(e){let i=Rt(e);i!==this._selected&&(this._setSelected(i),(i||this._selectionList.multiple)&&this._selectionList._reportValueChange())}_selected=!1;_inputsInitialized=!1;ngOnInit(){let e=this._selectionList;e._value&&e._value.some(r=>e.compareWith(this._value,r))&&this._setSelected(!0);let i=this._selected;Promise.resolve().then(()=>{(this._selected||i)&&(this.selected=!0,this._changeDetectorRef.markForCheck())}),this._inputsInitialized=!0}ngOnDestroy(){super.ngOnDestroy(),this.selected&&Promise.resolve().then(()=>{this.selected=!1})}toggle(){this.selected=!this.selected}focus(){this._hostElement.focus()}getLabel(){return(this._titles?.get(0)?._elementRef.nativeElement||this._unscopedContent?.nativeElement)?.textContent||""}_hasCheckboxAt(e){return this._selectionList.multiple&&this._getTogglePosition()===e}_hasRadioAt(e){return!this._selectionList.multiple&&this._getTogglePosition()===e&&!this._selectionList.hideSingleSelectionIndicator}_hasIconsOrAvatarsAt(e){return this._hasProjected("icons",e)||this._hasProjected("avatars",e)}_hasProjected(e,i){return this._getTogglePosition()!==i&&(e==="avatars"?this._avatars.length!==0:this._icons.length!==0)}_handleBlur(){this._selectionList._onTouched()}_getTogglePosition(){return this.togglePosition||"after"}_setSelected(e){return e===this._selected?!1:(this._selected=e,e?this._selectionList.selectedOptions.select(this):this._selectionList.selectedOptions.deselect(this),this.selectedChange.emit(e),this._changeDetectorRef.markForCheck(),!0)}_markForCheck(){this._changeDetectorRef.markForCheck()}_toggleOnInteraction(){this.disabled||(this._selectionList.multiple?(this.selected=!this.selected,this._selectionList._emitChangeEvent([this])):this.selected||(this.selected=!0,this._selectionList._emitChangeEvent([this])))}_setTabindex(e){this._hostElement.setAttribute("tabindex",e+"")}_hasBothLeadingAndTrailing(){let e=this._hasProjected("avatars","before")||this._hasProjected("icons","before")||this._hasCheckboxAt("before")||this._hasRadioAt("before"),i=this._hasProjected("icons","after")||this._hasProjected("avatars","after")||this._hasCheckboxAt("after")||this._hasRadioAt("after");return e&&i}static \u0275fac=(()=>{let e;return function(r){return(e||(e=ct(t)))(r||t)}})();static \u0275cmp=E({type:t,selectors:[["mat-list-option"]],contentQueries:function(i,r,o){if(i&1&&yt(o,nj,5)(o,tj,5),i&2){let s;$(s=G())&&(r._lines=s),$(s=G())&&(r._titles=s)}},viewQuery:function(i,r){if(i&1&&Ve(L2,5),i&2){let o;$(o=G())&&(r._unscopedContent=o.first)}},hostAttrs:["role","option",1,"mat-mdc-list-item","mat-mdc-list-option","mdc-list-item"],hostVars:27,hostBindings:function(i,r){i&1&&T("blur",function(){return r._handleBlur()})("click",function(){return r._toggleOnInteraction()}),i&2&&(q("aria-selected",r.selected),z("mdc-list-item--selected",r.selected&&!r._selectionList.multiple&&r._selectionList.hideSingleSelectionIndicator)("mdc-list-item--with-leading-avatar",r._hasProjected("avatars","before"))("mdc-list-item--with-leading-icon",r._hasProjected("icons","before"))("mdc-list-item--with-trailing-icon",r._hasProjected("icons","after"))("mat-mdc-list-option-with-trailing-avatar",r._hasProjected("avatars","after"))("mdc-list-item--with-leading-checkbox",r._hasCheckboxAt("before"))("mdc-list-item--with-trailing-checkbox",r._hasCheckboxAt("after"))("mdc-list-item--with-leading-radio",r._hasRadioAt("before"))("mdc-list-item--with-trailing-radio",r._hasRadioAt("after"))("mat-mdc-list-item-both-leading-and-trailing",r._hasBothLeadingAndTrailing())("mat-accent",r.color!=="primary"&&r.color!=="warn")("mat-warn",r.color==="warn")("_mat-animation-noopable",r._noopAnimations))},inputs:{togglePosition:"togglePosition",checkboxPosition:"checkboxPosition",color:"color",value:"value",selected:"selected"},outputs:{selectedChange:"selectedChange"},exportAs:["matListOption"],features:[Ee([{provide:OI,useExisting:t},{provide:NI,useExisting:t}]),fe],ngContentSelectors:B2,decls:20,vars:4,consts:[["icons",""],["checkbox",""],["radio",""],["unscopedContent",""],[1,"mdc-list-item__start","mat-mdc-list-option-checkbox-before"],[1,"mdc-list-item__start","mat-mdc-list-option-radio-before"],[3,"ngTemplateOutlet"],[1,"mdc-list-item__content"],[1,"mat-mdc-list-item-unscoped-content",3,"cdkObserveContent"],[1,"mdc-list-item__end"],[1,"mat-focus-indicator"],[1,"mdc-checkbox"],["type","checkbox",1,"mdc-checkbox__native-control",3,"checked","disabled"],[1,"mdc-checkbox__background"],["viewBox","0 0 24 24","aria-hidden","true",1,"mdc-checkbox__checkmark"],["fill","none","d","M1.73,12.91 8.1,19.28 22.79,4.59",1,"mdc-checkbox__checkmark-path"],[1,"mdc-checkbox__mixedmark"],[1,"mdc-radio"],["type","radio",1,"mdc-radio__native-control",3,"checked","disabled"],[1,"mdc-radio__background"],[1,"mdc-radio__outer-circle"],[1,"mdc-radio__inner-circle"]],template:function(i,r){i&1&&(Ce(j2),$e(0,V2,1,0,"ng-template",null,0,Xn)(2,H2,6,4,"ng-template",null,1,Xn)(4,U2,5,4,"ng-template",null,2,Xn),N(6,$2,2,1,"span",4)(7,W2,2,1,"span",5),N(8,Y2,1,1,null,6),f(9,"span",7),re(10),re(11,1),f(12,"span",8,3),T("cdkObserveContent",function(){return r._updateItemLines(!0)}),re(14,2),h()(),N(15,Q2,2,1,"span",9)(16,K2,2,1,"span",9),N(17,ej,1,1,null,6),re(18,3),O(19,"div",10)),i&2&&(p(6),P(r._hasCheckboxAt("before")?6:r._hasRadioAt("before")?7:-1),p(2),P(r._hasIconsOrAvatarsAt("before")?8:-1),p(7),P(r._hasCheckboxAt("after")?15:r._hasRadioAt("after")?16:-1),p(2),P(r._hasIconsOrAvatarsAt("after")?17:-1))},dependencies:[qi,dS],styles:[`.mat-mdc-list-option-with-trailing-avatar.mdc-list-item, [dir=rtl] .mat-mdc-list-option-with-trailing-avatar.mdc-list-item {
  padding-left: 0;
  padding-right: 0;
}
.mat-mdc-list-option-with-trailing-avatar .mdc-list-item__end {
  margin-left: 16px;
  margin-right: 16px;
  width: 40px;
  height: 40px;
}
.mat-mdc-list-option-with-trailing-avatar.mdc-list-item--with-two-lines .mdc-list-item__primary-text {
  display: block;
  margin-top: 0;
  line-height: normal;
  margin-bottom: -20px;
}
.mat-mdc-list-option-with-trailing-avatar.mdc-list-item--with-two-lines .mdc-list-item__primary-text::before {
  display: inline-block;
  width: 0;
  height: 32px;
  content: "";
  vertical-align: 0;
}
.mat-mdc-list-option-with-trailing-avatar.mdc-list-item--with-two-lines .mdc-list-item__primary-text::after {
  display: inline-block;
  width: 0;
  height: 20px;
  content: "";
  vertical-align: -20px;
}
.mat-mdc-list-option-with-trailing-avatar .mdc-list-item__end {
  border-radius: 50%;
}

.mat-mdc-list-option .mdc-checkbox {
  display: inline-block;
  position: relative;
  flex: 0 0 18px;
  box-sizing: content-box;
  width: 18px;
  height: 18px;
  line-height: 0;
  white-space: nowrap;
  cursor: pointer;
  vertical-align: bottom;
  padding: calc((var(--mat-checkbox-state-layer-size, 40px) - 18px) / 2);
  margin: calc((var(--mat-checkbox-state-layer-size, 40px) - var(--mat-checkbox-state-layer-size, 40px)) / 2);
}
.mat-mdc-list-option .mdc-checkbox .mdc-checkbox__native-control {
  position: absolute;
  margin: 0;
  padding: 0;
  opacity: 0;
  cursor: inherit;
  z-index: 1;
  width: var(--mat-checkbox-state-layer-size, 40px);
  height: var(--mat-checkbox-state-layer-size, 40px);
  top: calc((var(--mat-checkbox-state-layer-size, 40px) - var(--mat-checkbox-state-layer-size, 40px)) / 2);
  right: calc((var(--mat-checkbox-state-layer-size, 40px) - var(--mat-checkbox-state-layer-size, 40px)) / 2);
  left: calc((var(--mat-checkbox-state-layer-size, 40px) - var(--mat-checkbox-state-layer-size, 40px)) / 2);
}
.mat-mdc-list-option .mdc-checkbox--disabled {
  cursor: default;
  pointer-events: none;
}
.mat-mdc-list-option .mdc-checkbox__background {
  display: inline-flex;
  position: absolute;
  align-items: center;
  justify-content: center;
  box-sizing: border-box;
  width: 18px;
  height: 18px;
  border: 2px solid currentColor;
  border-radius: 2px;
  background-color: transparent;
  pointer-events: none;
  will-change: background-color, border-color;
  transition: background-color 90ms cubic-bezier(0.4, 0, 0.6, 1), border-color 90ms cubic-bezier(0.4, 0, 0.6, 1);
  -webkit-print-color-adjust: exact;
  color-adjust: exact;
  border-color: var(--mat-checkbox-unselected-icon-color, var(--mat-sys-on-surface-variant));
  top: calc((var(--mat-checkbox-state-layer-size, 40px) - 18px) / 2);
  left: calc((var(--mat-checkbox-state-layer-size, 40px) - 18px) / 2);
}
.mat-mdc-list-option .mdc-checkbox__native-control:enabled:checked ~ .mdc-checkbox__background,
.mat-mdc-list-option .mdc-checkbox__native-control:enabled:indeterminate ~ .mdc-checkbox__background {
  border-color: var(--mat-checkbox-selected-icon-color, var(--mat-sys-primary));
  background-color: var(--mat-checkbox-selected-icon-color, var(--mat-sys-primary));
}
.mat-mdc-list-option .mdc-checkbox--disabled .mdc-checkbox__background {
  border-color: var(--mat-checkbox-disabled-unselected-icon-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
}
@media (forced-colors: active) {
  .mat-mdc-list-option .mdc-checkbox--disabled .mdc-checkbox__background {
    border-color: GrayText;
  }
}
.mat-mdc-list-option .mdc-checkbox__native-control:disabled:checked ~ .mdc-checkbox__background,
.mat-mdc-list-option .mdc-checkbox__native-control:disabled:indeterminate ~ .mdc-checkbox__background {
  background-color: var(--mat-checkbox-disabled-selected-icon-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
  border-color: transparent;
}
@media (forced-colors: active) {
  .mat-mdc-list-option .mdc-checkbox__native-control:disabled:checked ~ .mdc-checkbox__background,
  .mat-mdc-list-option .mdc-checkbox__native-control:disabled:indeterminate ~ .mdc-checkbox__background {
    border-color: GrayText;
  }
}
.mat-mdc-list-option .mdc-checkbox:hover > .mdc-checkbox__native-control:not(:checked) ~ .mdc-checkbox__background,
.mat-mdc-list-option .mdc-checkbox:hover > .mdc-checkbox__native-control:not(:indeterminate) ~ .mdc-checkbox__background {
  border-color: var(--mat-checkbox-unselected-hover-icon-color, var(--mat-sys-on-surface));
  background-color: transparent;
}
.mat-mdc-list-option .mdc-checkbox:hover > .mdc-checkbox__native-control:checked ~ .mdc-checkbox__background,
.mat-mdc-list-option .mdc-checkbox:hover > .mdc-checkbox__native-control:indeterminate ~ .mdc-checkbox__background {
  border-color: var(--mat-checkbox-selected-hover-icon-color, var(--mat-sys-primary));
  background-color: var(--mat-checkbox-selected-hover-icon-color, var(--mat-sys-primary));
}
.mat-mdc-list-option .mdc-checkbox__native-control:focus:focus:not(:checked) ~ .mdc-checkbox__background,
.mat-mdc-list-option .mdc-checkbox__native-control:focus:focus:not(:indeterminate) ~ .mdc-checkbox__background {
  border-color: var(--mat-checkbox-unselected-focus-icon-color, var(--mat-sys-on-surface));
}
.mat-mdc-list-option .mdc-checkbox__native-control:focus:focus:checked ~ .mdc-checkbox__background,
.mat-mdc-list-option .mdc-checkbox__native-control:focus:focus:indeterminate ~ .mdc-checkbox__background {
  border-color: var(--mat-checkbox-selected-focus-icon-color, var(--mat-sys-primary));
  background-color: var(--mat-checkbox-selected-focus-icon-color, var(--mat-sys-primary));
}
.mat-mdc-list-option .mdc-checkbox--disabled.mat-mdc-checkbox-disabled-interactive .mdc-checkbox:hover > .mdc-checkbox__native-control ~ .mdc-checkbox__background,
.mat-mdc-list-option .mdc-checkbox--disabled.mat-mdc-checkbox-disabled-interactive .mdc-checkbox .mdc-checkbox__native-control:focus ~ .mdc-checkbox__background,
.mat-mdc-list-option .mdc-checkbox--disabled.mat-mdc-checkbox-disabled-interactive .mdc-checkbox__background {
  border-color: var(--mat-checkbox-disabled-unselected-icon-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
}
@media (forced-colors: active) {
  .mat-mdc-list-option .mdc-checkbox--disabled.mat-mdc-checkbox-disabled-interactive .mdc-checkbox:hover > .mdc-checkbox__native-control ~ .mdc-checkbox__background,
  .mat-mdc-list-option .mdc-checkbox--disabled.mat-mdc-checkbox-disabled-interactive .mdc-checkbox .mdc-checkbox__native-control:focus ~ .mdc-checkbox__background,
  .mat-mdc-list-option .mdc-checkbox--disabled.mat-mdc-checkbox-disabled-interactive .mdc-checkbox__background {
    border-color: GrayText;
  }
}
.mat-mdc-list-option .mdc-checkbox--disabled.mat-mdc-checkbox-disabled-interactive .mdc-checkbox__native-control:checked ~ .mdc-checkbox__background,
.mat-mdc-list-option .mdc-checkbox--disabled.mat-mdc-checkbox-disabled-interactive .mdc-checkbox__native-control:indeterminate ~ .mdc-checkbox__background {
  background-color: var(--mat-checkbox-disabled-selected-icon-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
  border-color: transparent;
}
.mat-mdc-list-option .mdc-checkbox__checkmark {
  position: absolute;
  top: 0;
  right: 0;
  bottom: 0;
  left: 0;
  width: 100%;
  opacity: 0;
  transition: opacity 180ms cubic-bezier(0.4, 0, 0.6, 1);
  color: var(--mat-checkbox-selected-checkmark-color, var(--mat-sys-on-primary));
}
@media (forced-colors: active) {
  .mat-mdc-list-option .mdc-checkbox__checkmark {
    color: CanvasText;
  }
}
.mat-mdc-list-option .mdc-checkbox--disabled .mdc-checkbox__checkmark, .mat-mdc-list-option .mdc-checkbox--disabled.mat-mdc-checkbox-disabled-interactive .mdc-checkbox__checkmark {
  color: var(--mat-checkbox-disabled-selected-checkmark-color, var(--mat-sys-surface));
}
@media (forced-colors: active) {
  .mat-mdc-list-option .mdc-checkbox--disabled .mdc-checkbox__checkmark, .mat-mdc-list-option .mdc-checkbox--disabled.mat-mdc-checkbox-disabled-interactive .mdc-checkbox__checkmark {
    color: GrayText;
  }
}
.mat-mdc-list-option .mdc-checkbox__checkmark-path {
  transition: stroke-dashoffset 180ms cubic-bezier(0.4, 0, 0.6, 1);
  stroke: currentColor;
  stroke-width: 3.12px;
  stroke-dashoffset: 29.7833385;
  stroke-dasharray: 29.7833385;
}
.mat-mdc-list-option .mdc-checkbox__mixedmark {
  width: 100%;
  height: 0;
  transform: scaleX(0) rotate(0deg);
  border-width: 1px;
  border-style: solid;
  opacity: 0;
  transition: opacity 90ms cubic-bezier(0.4, 0, 0.6, 1), transform 90ms cubic-bezier(0.4, 0, 0.6, 1);
  border-color: var(--mat-checkbox-selected-checkmark-color, var(--mat-sys-on-primary));
}
@media (forced-colors: active) {
  .mat-mdc-list-option .mdc-checkbox__mixedmark {
    margin: 0 1px;
  }
}
.mat-mdc-list-option .mdc-checkbox--disabled .mdc-checkbox__mixedmark, .mat-mdc-list-option .mdc-checkbox--disabled.mat-mdc-checkbox-disabled-interactive .mdc-checkbox__mixedmark {
  border-color: var(--mat-checkbox-disabled-selected-checkmark-color, var(--mat-sys-surface));
}
@media (forced-colors: active) {
  .mat-mdc-list-option .mdc-checkbox--disabled .mdc-checkbox__mixedmark, .mat-mdc-list-option .mdc-checkbox--disabled.mat-mdc-checkbox-disabled-interactive .mdc-checkbox__mixedmark {
    border-color: GrayText;
  }
}
.mat-mdc-list-option .mdc-checkbox--anim-unchecked-checked .mdc-checkbox__background,
.mat-mdc-list-option .mdc-checkbox--anim-unchecked-indeterminate .mdc-checkbox__background,
.mat-mdc-list-option .mdc-checkbox--anim-checked-unchecked .mdc-checkbox__background,
.mat-mdc-list-option .mdc-checkbox--anim-indeterminate-unchecked .mdc-checkbox__background {
  animation-duration: 180ms;
  animation-timing-function: linear;
}
.mat-mdc-list-option .mdc-checkbox--anim-unchecked-checked .mdc-checkbox__checkmark-path {
  animation: mdc-checkbox-unchecked-checked-checkmark-path 180ms linear;
  transition: none;
}
.mat-mdc-list-option .mdc-checkbox--anim-unchecked-indeterminate .mdc-checkbox__mixedmark {
  animation: mdc-checkbox-unchecked-indeterminate-mixedmark 90ms linear;
  transition: none;
}
.mat-mdc-list-option .mdc-checkbox--anim-checked-unchecked .mdc-checkbox__checkmark-path {
  animation: mdc-checkbox-checked-unchecked-checkmark-path 90ms linear;
  transition: none;
}
.mat-mdc-list-option .mdc-checkbox--anim-checked-indeterminate .mdc-checkbox__checkmark {
  animation: mdc-checkbox-checked-indeterminate-checkmark 90ms linear;
  transition: none;
}
.mat-mdc-list-option .mdc-checkbox--anim-checked-indeterminate .mdc-checkbox__mixedmark {
  animation: mdc-checkbox-checked-indeterminate-mixedmark 90ms linear;
  transition: none;
}
.mat-mdc-list-option .mdc-checkbox--anim-indeterminate-checked .mdc-checkbox__checkmark {
  animation: mdc-checkbox-indeterminate-checked-checkmark 500ms linear;
  transition: none;
}
.mat-mdc-list-option .mdc-checkbox--anim-indeterminate-checked .mdc-checkbox__mixedmark {
  animation: mdc-checkbox-indeterminate-checked-mixedmark 500ms linear;
  transition: none;
}
.mat-mdc-list-option .mdc-checkbox--anim-indeterminate-unchecked .mdc-checkbox__mixedmark {
  animation: mdc-checkbox-indeterminate-unchecked-mixedmark 300ms linear;
  transition: none;
}
.mat-mdc-list-option .mdc-checkbox__native-control:checked ~ .mdc-checkbox__background,
.mat-mdc-list-option .mdc-checkbox__native-control:indeterminate ~ .mdc-checkbox__background {
  transition: border-color 90ms cubic-bezier(0, 0, 0.2, 1), background-color 90ms cubic-bezier(0, 0, 0.2, 1);
}
.mat-mdc-list-option .mdc-checkbox__native-control:checked ~ .mdc-checkbox__background > .mdc-checkbox__checkmark > .mdc-checkbox__checkmark-path,
.mat-mdc-list-option .mdc-checkbox__native-control:indeterminate ~ .mdc-checkbox__background > .mdc-checkbox__checkmark > .mdc-checkbox__checkmark-path {
  stroke-dashoffset: 0;
}
.mat-mdc-list-option .mdc-checkbox__native-control:checked ~ .mdc-checkbox__background > .mdc-checkbox__checkmark {
  transition: opacity 180ms cubic-bezier(0, 0, 0.2, 1), transform 180ms cubic-bezier(0, 0, 0.2, 1);
  opacity: 1;
}
.mat-mdc-list-option .mdc-checkbox__native-control:checked ~ .mdc-checkbox__background > .mdc-checkbox__mixedmark {
  transform: scaleX(1) rotate(-45deg);
}
.mat-mdc-list-option .mdc-checkbox__native-control:indeterminate ~ .mdc-checkbox__background > .mdc-checkbox__checkmark {
  transform: rotate(45deg);
  opacity: 0;
  transition: opacity 90ms cubic-bezier(0.4, 0, 0.6, 1), transform 90ms cubic-bezier(0.4, 0, 0.6, 1);
}
.mat-mdc-list-option .mdc-checkbox__native-control:indeterminate ~ .mdc-checkbox__background > .mdc-checkbox__mixedmark {
  transform: scaleX(1) rotate(0deg);
  opacity: 1;
}
@keyframes mdc-checkbox-unchecked-checked-checkmark-path {
  0%, 50% {
    stroke-dashoffset: 29.7833385;
  }
  50% {
    animation-timing-function: cubic-bezier(0, 0, 0.2, 1);
  }
  100% {
    stroke-dashoffset: 0;
  }
}
@keyframes mdc-checkbox-unchecked-indeterminate-mixedmark {
  0%, 68.2% {
    transform: scaleX(0);
  }
  68.2% {
    animation-timing-function: cubic-bezier(0, 0, 0, 1);
  }
  100% {
    transform: scaleX(1);
  }
}
@keyframes mdc-checkbox-checked-unchecked-checkmark-path {
  from {
    animation-timing-function: cubic-bezier(0.4, 0, 1, 1);
    opacity: 1;
    stroke-dashoffset: 0;
  }
  to {
    opacity: 0;
    stroke-dashoffset: -29.7833385;
  }
}
@keyframes mdc-checkbox-checked-indeterminate-checkmark {
  from {
    animation-timing-function: cubic-bezier(0, 0, 0.2, 1);
    transform: rotate(0deg);
    opacity: 1;
  }
  to {
    transform: rotate(45deg);
    opacity: 0;
  }
}
@keyframes mdc-checkbox-indeterminate-checked-checkmark {
  from {
    animation-timing-function: cubic-bezier(0.14, 0, 0, 1);
    transform: rotate(45deg);
    opacity: 0;
  }
  to {
    transform: rotate(360deg);
    opacity: 1;
  }
}
@keyframes mdc-checkbox-checked-indeterminate-mixedmark {
  from {
    animation-timing-function: cubic-bezier(0, 0, 0.2, 1);
    transform: rotate(-45deg);
    opacity: 0;
  }
  to {
    transform: rotate(0deg);
    opacity: 1;
  }
}
@keyframes mdc-checkbox-indeterminate-checked-mixedmark {
  from {
    animation-timing-function: cubic-bezier(0.14, 0, 0, 1);
    transform: rotate(0deg);
    opacity: 1;
  }
  to {
    transform: rotate(315deg);
    opacity: 0;
  }
}
@keyframes mdc-checkbox-indeterminate-unchecked-mixedmark {
  0% {
    animation-timing-function: linear;
    transform: scaleX(1);
    opacity: 1;
  }
  32.8%, 100% {
    transform: scaleX(0);
    opacity: 0;
  }
}
.mat-mdc-list-option .mdc-radio {
  display: inline-block;
  position: relative;
  flex: 0 0 auto;
  box-sizing: content-box;
  width: 20px;
  height: 20px;
  cursor: pointer;
  will-change: opacity, transform, border-color, color;
  padding: calc((var(--mat-radio-state-layer-size, 40px) - 20px) / 2);
}
.mat-mdc-list-option .mdc-radio__background {
  display: inline-block;
  position: relative;
  box-sizing: border-box;
  width: 20px;
  height: 20px;
}
.mat-mdc-list-option .mdc-radio__background::before {
  position: absolute;
  transform: scale(0, 0);
  border-radius: 50%;
  opacity: 0;
  pointer-events: none;
  content: "";
  transition: opacity 90ms cubic-bezier(0.4, 0, 0.6, 1), transform 90ms cubic-bezier(0.4, 0, 0.6, 1);
  width: var(--mat-radio-state-layer-size, 40px);
  height: var(--mat-radio-state-layer-size, 40px);
  top: calc(-1 * (var(--mat-radio-state-layer-size, 40px) - 20px) / 2);
  left: calc(-1 * (var(--mat-radio-state-layer-size, 40px) - 20px) / 2);
}
.mat-mdc-list-option .mdc-radio__outer-circle {
  position: absolute;
  top: 0;
  left: 0;
  box-sizing: border-box;
  width: 100%;
  height: 100%;
  border-width: 2px;
  border-style: solid;
  border-radius: 50%;
  transition: border-color 90ms cubic-bezier(0.4, 0, 0.6, 1);
}
.mat-mdc-list-option .mdc-radio__inner-circle {
  position: absolute;
  top: 0;
  left: 0;
  box-sizing: border-box;
  width: 100%;
  height: 100%;
  transform: scale(0);
  border-radius: 50%;
  transition: transform 90ms cubic-bezier(0.4, 0, 0.6, 1), background-color 90ms cubic-bezier(0.4, 0, 0.6, 1);
}
@media (forced-colors: active) {
  .mat-mdc-list-option .mdc-radio__inner-circle {
    background-color: CanvasText !important;
  }
}
.mat-mdc-list-option .mdc-radio__native-control {
  position: absolute;
  margin: 0;
  padding: 0;
  opacity: 0;
  top: 0;
  right: 0;
  left: 0;
  cursor: inherit;
  z-index: 1;
  width: var(--mat-radio-state-layer-size, 40px);
  height: var(--mat-radio-state-layer-size, 40px);
}
.mat-mdc-list-option .mdc-radio__native-control:checked + .mdc-radio__background, .mat-mdc-list-option .mdc-radio__native-control:disabled + .mdc-radio__background {
  transition: opacity 90ms cubic-bezier(0, 0, 0.2, 1), transform 90ms cubic-bezier(0, 0, 0.2, 1);
}
.mat-mdc-list-option .mdc-radio__native-control:checked + .mdc-radio__background > .mdc-radio__outer-circle, .mat-mdc-list-option .mdc-radio__native-control:disabled + .mdc-radio__background > .mdc-radio__outer-circle {
  transition: border-color 90ms cubic-bezier(0, 0, 0.2, 1);
}
.mat-mdc-list-option .mdc-radio__native-control:checked + .mdc-radio__background > .mdc-radio__inner-circle, .mat-mdc-list-option .mdc-radio__native-control:disabled + .mdc-radio__background > .mdc-radio__inner-circle {
  transition: transform 90ms cubic-bezier(0, 0, 0.2, 1), background-color 90ms cubic-bezier(0, 0, 0.2, 1);
}
.mat-mdc-list-option .mdc-radio__native-control:disabled:not(:checked) + .mdc-radio__background > .mdc-radio__outer-circle {
  border-color: var(--mat-radio-disabled-unselected-icon-color, var(--mat-sys-on-surface));
  opacity: var(--mat-radio-disabled-unselected-icon-opacity, 0.38);
}
.mat-mdc-list-option .mdc-radio__native-control:disabled + .mdc-radio__background {
  cursor: default;
}
.mat-mdc-list-option .mdc-radio__native-control:disabled + .mdc-radio__background > .mdc-radio__outer-circle {
  border-color: var(--mat-radio-disabled-selected-icon-color, var(--mat-sys-on-surface));
  opacity: var(--mat-radio-disabled-selected-icon-opacity, 0.38);
}
.mat-mdc-list-option .mdc-radio__native-control:disabled + .mdc-radio__background > .mdc-radio__inner-circle {
  background-color: var(--mat-radio-disabled-selected-icon-color, var(--mat-sys-on-surface, currentColor));
  opacity: var(--mat-radio-disabled-selected-icon-opacity, 0.38);
}
.mat-mdc-list-option .mdc-radio__native-control:enabled:not(:checked) + .mdc-radio__background > .mdc-radio__outer-circle {
  border-color: var(--mat-radio-unselected-icon-color, var(--mat-sys-on-surface-variant));
}
.mat-mdc-list-option .mdc-radio__native-control:enabled:checked + .mdc-radio__background > .mdc-radio__outer-circle {
  border-color: var(--mat-radio-selected-icon-color, var(--mat-sys-primary));
}
.mat-mdc-list-option .mdc-radio__native-control:enabled:checked + .mdc-radio__background > .mdc-radio__inner-circle {
  background-color: var(--mat-radio-selected-icon-color, var(--mat-sys-primary, currentColor));
}
.mat-mdc-list-option .mdc-radio__native-control:checked + .mdc-radio__background > .mdc-radio__inner-circle {
  transform: scale(0.5);
  transition: transform 90ms cubic-bezier(0, 0, 0.2, 1), background-color 90ms cubic-bezier(0, 0, 0.2, 1);
}
.mat-mdc-list-option._mat-animation-noopable .mdc-radio__background::before,
.mat-mdc-list-option._mat-animation-noopable .mdc-radio__outer-circle,
.mat-mdc-list-option._mat-animation-noopable .mdc-radio__inner-circle {
  transition: none !important;
}
.mat-mdc-list-option._mat-animation-noopable > .mdc-list-item__start > .mdc-checkbox > .mat-mdc-checkbox-touch-target,
.mat-mdc-list-option._mat-animation-noopable > .mdc-list-item__start > .mdc-checkbox > .mdc-checkbox__native-control,
.mat-mdc-list-option._mat-animation-noopable > .mdc-list-item__start > .mdc-checkbox > .mdc-checkbox__ripple,
.mat-mdc-list-option._mat-animation-noopable > .mdc-list-item__start > .mdc-checkbox > .mat-mdc-checkbox-ripple::before,
.mat-mdc-list-option._mat-animation-noopable > .mdc-list-item__start > .mdc-checkbox > .mdc-checkbox__background,
.mat-mdc-list-option._mat-animation-noopable > .mdc-list-item__start > .mdc-checkbox > .mdc-checkbox__background > .mdc-checkbox__checkmark,
.mat-mdc-list-option._mat-animation-noopable > .mdc-list-item__start > .mdc-checkbox > .mdc-checkbox__background > .mdc-checkbox__checkmark > .mdc-checkbox__checkmark-path,
.mat-mdc-list-option._mat-animation-noopable > .mdc-list-item__start > .mdc-checkbox > .mdc-checkbox__background > .mdc-checkbox__mixedmark, .mat-mdc-list-option._mat-animation-noopable > .mdc-list-item__end > .mdc-checkbox > .mat-mdc-checkbox-touch-target,
.mat-mdc-list-option._mat-animation-noopable > .mdc-list-item__end > .mdc-checkbox > .mdc-checkbox__native-control,
.mat-mdc-list-option._mat-animation-noopable > .mdc-list-item__end > .mdc-checkbox > .mdc-checkbox__ripple,
.mat-mdc-list-option._mat-animation-noopable > .mdc-list-item__end > .mdc-checkbox > .mat-mdc-checkbox-ripple::before,
.mat-mdc-list-option._mat-animation-noopable > .mdc-list-item__end > .mdc-checkbox > .mdc-checkbox__background,
.mat-mdc-list-option._mat-animation-noopable > .mdc-list-item__end > .mdc-checkbox > .mdc-checkbox__background > .mdc-checkbox__checkmark,
.mat-mdc-list-option._mat-animation-noopable > .mdc-list-item__end > .mdc-checkbox > .mdc-checkbox__background > .mdc-checkbox__checkmark > .mdc-checkbox__checkmark-path,
.mat-mdc-list-option._mat-animation-noopable > .mdc-list-item__end > .mdc-checkbox > .mdc-checkbox__background > .mdc-checkbox__mixedmark {
  transition: none !important;
  animation: none !important;
}
.mat-mdc-list-option .mdc-checkbox__native-control, .mat-mdc-list-option .mdc-radio__native-control {
  display: none;
}

@media (forced-colors: active) {
  .mat-mdc-list-option.mdc-list-item--selected::after {
    content: "";
    position: absolute;
    top: 50%;
    right: 16px;
    transform: translateY(-50%);
    width: 10px;
    height: 0;
    border-bottom: solid 10px;
    border-radius: 10px;
  }
  .mat-mdc-list-option.mdc-list-item--selected [dir=rtl]::after {
    right: auto;
    left: 16px;
  }
}
`],encapsulation:2,changeDetection:0})}return t})();var sj={provide:Ei,useExisting:It(()=>ld),multi:!0},ry=class{source;options;constructor(n,e){this.source=n,this.options=e}},ld=(()=>{class t extends iy{_element=d(L);_ngZone=d(H);_renderer=d(xe);_initialized=!1;_keyManager;_listenerCleanups;_destroyed=new I;_isDestroyed=!1;_onChange=e=>{};_items;selectionChange=new U;color="accent";compareWith=(e,i)=>e===i;get multiple(){return this._multiple}set multiple(e){let i=Rt(e);i!==this._multiple&&(this._multiple=i,this.selectedOptions=new mo(this._multiple,this.selectedOptions.selected))}_multiple=!0;get hideSingleSelectionIndicator(){return this._hideSingleSelectionIndicator}set hideSingleSelectionIndicator(e){this._hideSingleSelectionIndicator=Rt(e)}_hideSingleSelectionIndicator=this._defaultOptions?.hideSingleSelectionIndicator??!1;selectedOptions=new mo(this._multiple);_value=null;_onTouched=()=>{};_changeDetectorRef=d(ve);constructor(){super(),this._isNonInteractive=!1}ngAfterViewInit(){this._initialized=!0,this._setupRovingTabindex(),this._ngZone.runOutsideAngular(()=>{this._listenerCleanups=[this._renderer.listen(this._element.nativeElement,"focusin",this._handleFocusin),this._renderer.listen(this._element.nativeElement,"focusout",this._handleFocusout)]}),this._value&&this._setOptionsFromValues(this._value),this._watchForSelectionChange()}ngOnChanges(e){let i=e.disabled,r=e.disableRipple,o=e.hideSingleSelectionIndicator;(r&&!r.firstChange||i&&!i.firstChange||o&&!o.firstChange)&&this._markOptionsForCheck()}ngOnDestroy(){this._keyManager?.destroy(),this._listenerCleanups?.forEach(e=>e()),this._destroyed.next(),this._destroyed.complete(),this._isDestroyed=!0}focus(e){this._element.nativeElement.focus(e)}selectAll(){return this._setAllOptionsSelected(!0)}deselectAll(){return this._setAllOptionsSelected(!1)}_reportValueChange(){if(this.options&&!this._isDestroyed){let e=this._getSelectedOptionValues();this._onChange(e),this._value=e}}_emitChangeEvent(e){this.selectionChange.emit(new ry(this,e))}writeValue(e){this._value=e,this.options&&this._setOptionsFromValues(e||[])}setDisabledState(e){this.disabled=e,this._changeDetectorRef.markForCheck(),this._markOptionsForCheck()}get disabled(){return this._selectionListDisabled()}set disabled(e){this._selectionListDisabled.set(Rt(e)),this._selectionListDisabled()&&this._keyManager?.setActiveItem(-1)}_selectionListDisabled=M(!1);registerOnChange(e){this._onChange=e}registerOnTouched(e){this._onTouched=e}_watchForSelectionChange(){this.selectedOptions.changed.pipe(me(this._destroyed)).subscribe(e=>{for(let i of e.added)i.selected=!0;for(let i of e.removed)i.selected=!1;this._containsFocus()||this._resetActiveOption()})}_setOptionsFromValues(e){this.options.forEach(i=>i._setSelected(!1)),e.forEach(i=>{let r=this.options.find(o=>o.selected?!1:this.compareWith(o.value,i));r&&r._setSelected(!0)})}_getSelectedOptionValues(){return this.options.filter(e=>e.selected).map(e=>e.value)}_markOptionsForCheck(){this.options&&this.options.forEach(e=>e._markForCheck())}_setAllOptionsSelected(e,i){let r=[];return this.options.forEach(o=>{(!i||!o.disabled)&&o._setSelected(e)&&r.push(o)}),r.length&&this._reportValueChange(),r}get options(){return this._items}_handleKeydown(e){let i=this._keyManager.activeItem;if((e.keyCode===13||e.keyCode===32)&&!this._keyManager.isTyping()&&i&&!i.disabled)e.preventDefault(),i._toggleOnInteraction();else if(e.keyCode===65&&this.multiple&&!this._keyManager.isTyping()&&Ct(e,"ctrlKey","metaKey")){let r=this.options.some(o=>!o.disabled&&!o.selected);e.preventDefault(),this._emitChangeEvent(this._setAllOptionsSelected(r,!0))}else this._keyManager.onKeydown(e)}_handleFocusout=()=>{setTimeout(()=>{this._containsFocus()||this._resetActiveOption()})};_handleFocusin=e=>{if(this.disabled)return;let i=this._items.toArray().findIndex(r=>r._elementRef.nativeElement.contains(e.target));i>-1?this._setActiveOption(i):this._resetActiveOption()};_setupRovingTabindex(){this._keyManager=new xr(this._items).withHomeAndEnd().withTypeAhead().withWrap().skipPredicate(()=>this.disabled),this._resetActiveOption(),this._keyManager.change.subscribe(e=>this._setActiveOption(e)),this._items.changes.pipe(me(this._destroyed)).subscribe(()=>{let e=this._keyManager.activeItem;(!e||this._items.toArray().indexOf(e)===-1)&&this._resetActiveOption()})}_setActiveOption(e){this._items.forEach((i,r)=>i._setTabindex(r===e?0:-1)),this._keyManager.updateActiveItem(e)}_resetActiveOption(){if(this.disabled){this._setActiveOption(-1);return}let e=this._items.find(i=>i.selected&&!i.disabled)||this._items.first;this._setActiveOption(e?this._items.toArray().indexOf(e):-1)}_containsFocus(){let e=yr();return e&&this._element.nativeElement.contains(e)}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=E({type:t,selectors:[["mat-selection-list"]],contentQueries:function(i,r,o){if(i&1&&yt(o,cd,5),i&2){let s;$(s=G())&&(r._items=s)}},hostAttrs:["role","listbox",1,"mat-mdc-selection-list","mat-mdc-list-base","mdc-list"],hostVars:1,hostBindings:function(i,r){i&1&&T("keydown",function(s){return r._handleKeydown(s)}),i&2&&q("aria-multiselectable",r.multiple)},inputs:{color:"color",compareWith:"compareWith",multiple:"multiple",hideSingleSelectionIndicator:"hideSingleSelectionIndicator",disabled:"disabled"},outputs:{selectionChange:"selectionChange"},exportAs:["matSelectionList"],features:[Ee([sj,{provide:iy,useExisting:t},{provide:FI,useExisting:t}]),fe,ze],ngContentSelectors:P2,decls:1,vars:0,template:function(i,r){i&1&&(Ce(),re(0))},styles:[F2],encapsulation:2,changeDetection:0})}return t})();function aj(t,n){if(t&1&&(f(0,"mat-list-option",5),_(1),h()),t&2){let e=n.$implicit;D("value",e),p(),V(e)}}var th=class t{shopService=d(so);dialogRef=d(Xl);data=d(jb);selectedGenres=this.data.selectedGenres;applyFilters(){this.dialogRef.close({selectedGenres:this.selectedGenres})}clearFilters(){this.selectedGenres=[]}static \u0275fac=function(e){return new(e||t)};static \u0275cmp=E({type:t,selectors:[["app-filters-dialog"]],decls:16,vars:2,consts:[[1,"text-3xl","text-center","pt-6","mb-3"],[1,"flex","p-4"],[1,"w-full"],[1,"font-semibold","text-xl","mb-2"],[3,"ngModelChange","ngModel","multiple"],[3,"value"],[1,"flex","justify-end","p-4","gap-2"],["mat-stroked-button","",3,"click"],["mat-flat-button","",3,"click"]],template:function(e,i){e&1&&(f(0,"div")(1,"h3",0),_(2,"Filters"),h(),O(3,"mat-divider"),f(4,"div",1)(5,"div",2)(6,"h4",3),_(7,"Genre"),h(),f(8,"mat-selection-list",4),Jr("ngModelChange",function(o){return is(i.selectedGenres,o)||(i.selectedGenres=o),o}),tt(9,aj,2,2,"mat-list-option",5,vi),h()()(),f(11,"div",6)(12,"button",7),T("click",function(){return i.clearFilters()}),_(13,"Clear Filters"),h(),f(14,"button",8),T("click",function(){return i.applyFilters()}),_(15,"Apply Filters"),h()()()),e&2&&(p(8),Kr("ngModel",i.selectedGenres),D("multiple",!0),p(),nt(i.shopService.genres))},dependencies:[Cs,ld,cd,Pe,Ua,ii,xs],encapsulation:2})};var cj=["mat-menu-item",""],lj=[[["mat-icon"],["","matMenuItemIcon",""]],"*"],dj=["mat-icon, [matMenuItemIcon]","*"];function uj(t,n){t&1&&(Gt(),f(0,"svg",2),O(1,"polygon",3),h())}var mj=["*"];function fj(t,n){if(t&1){let e=Mt();Fe(0,"div",0),na("click",function(){Te(e);let r=S();return Ae(r.closed.emit("click"))})("animationstart",function(r){Te(e);let o=S();return Ae(o._onAnimationStart(r.animationName))})("animationend",function(r){Te(e);let o=S();return Ae(o._onAnimationDone(r.animationName))})("animationcancel",function(r){Te(e);let o=S();return Ae(o._onAnimationDone(r.animationName))}),Fe(1,"div",1),re(2),Ge()()}if(t&2){let e=S();Tt(e._classList),z("mat-menu-panel-animations-disabled",e._animationsDisabled)("mat-menu-panel-exit-animation",e._panelAnimationState==="void")("mat-menu-panel-animating",e._isAnimating()),mt("id",e.panelId),q("aria-label",e.ariaLabel||null)("aria-labelledby",e.ariaLabelledby||null)("aria-describedby",e.ariaDescribedby||null)}}var sy=new y("MAT_MENU_PANEL"),dd=(()=>{class t{_elementRef=d(L);_document=d(K);_focusMonitor=d(Tn);_parentMenu=d(sy,{optional:!0});_changeDetectorRef=d(ve);role="menuitem";disabled=!1;disableRipple=!1;_hovered=new I;_focused=new I;_highlighted=!1;_triggersSubmenu=!1;constructor(){d(qe).load(An),this._parentMenu?.addItem?.(this)}focus(e,i){this._focusMonitor&&e?this._focusMonitor.focusVia(this._getHostElement(),e,i):this._getHostElement().focus(i),this._focused.next(this)}ngAfterViewInit(){this._focusMonitor&&this._focusMonitor.monitor(this._elementRef,!1)}ngOnDestroy(){this._focusMonitor&&this._focusMonitor.stopMonitoring(this._elementRef),this._parentMenu&&this._parentMenu.removeItem&&this._parentMenu.removeItem(this),this._hovered.complete(),this._focused.complete()}_getTabIndex(){return this.disabled?"-1":"0"}_getHostElement(){return this._elementRef.nativeElement}_checkDisabled(e){this.disabled&&(e.preventDefault(),e.stopPropagation())}_handleMouseEnter(){this._hovered.next(this)}getLabel(){let e=this._elementRef.nativeElement.cloneNode(!0),i=e.querySelectorAll("mat-icon, .material-icons");for(let r=0;r<i.length;r++)i[r].remove();return e.textContent?.trim()||""}_setHighlighted(e){this._highlighted=e,this._changeDetectorRef.markForCheck()}_setTriggersSubmenu(e){this._triggersSubmenu=e,this._changeDetectorRef.markForCheck()}_hasFocus(){return this._document&&this._document.activeElement===this._getHostElement()}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=E({type:t,selectors:[["","mat-menu-item",""]],hostAttrs:[1,"mat-mdc-menu-item","mat-focus-indicator"],hostVars:8,hostBindings:function(i,r){i&1&&T("click",function(s){return r._checkDisabled(s)})("mouseenter",function(){return r._handleMouseEnter()}),i&2&&(q("role",r.role)("tabindex",r._getTabIndex())("aria-disabled",r.disabled)("disabled",r.disabled||null),z("mat-mdc-menu-item-highlighted",r._highlighted)("mat-mdc-menu-item-submenu-trigger",r._triggersSubmenu))},inputs:{role:"role",disabled:[2,"disabled","disabled",B],disableRipple:[2,"disableRipple","disableRipple",B]},exportAs:["matMenuItem"],attrs:cj,ngContentSelectors:dj,decls:5,vars:3,consts:[[1,"mat-mdc-menu-item-text"],["matRipple","",1,"mat-mdc-menu-ripple",3,"matRippleDisabled","matRippleTrigger"],["viewBox","0 0 5 10","focusable","false","aria-hidden","true",1,"mat-mdc-menu-submenu-icon"],["points","0,0 5,5 0,10"]],template:function(i,r){i&1&&(Ce(lj),re(0),f(1,"span",0),re(2,1),h(),O(3,"div",1),N(4,uj,2,0,":svg:svg",2)),i&2&&(p(3),D("matRippleDisabled",r.disableRipple||r.disabled)("matRippleTrigger",r._getHostElement()),p(),P(r._triggersSubmenu?4:-1))},dependencies:[Xi],encapsulation:2,changeDetection:0})}return t})();var hj=new y("MatMenuContent");var pj=new y("mat-menu-default-options",{providedIn:"root",factory:()=>({overlapTrigger:!1,xPosition:"after",yPosition:"below",backdropClass:"cdk-overlay-transparent-backdrop"})}),oy="_mat-menu-enter",nh="_mat-menu-exit",po=(()=>{class t{_elementRef=d(L);_changeDetectorRef=d(ve);_injector=d(X);_keyManager;_xPosition;_yPosition;_firstItemFocusRef;_exitFallbackTimeout;_animationsDisabled=ke();_allItems;_directDescendantItems=new En;_classList={};_panelAnimationState="void";_animationDone=new I;_isAnimating=M(!1);parentMenu;direction;overlayPanelClass;backdropClass;ariaLabel;ariaLabelledby;ariaDescribedby;get xPosition(){return this._xPosition}set xPosition(e){this._xPosition=e,this.setPositionClasses()}get yPosition(){return this._yPosition}set yPosition(e){this._yPosition=e,this.setPositionClasses()}templateRef;items;lazyContent;overlapTrigger=!1;hasBackdrop;set panelClass(e){let i=this._previousPanelClass,r=b({},this._classList);i&&i.length&&i.split(" ").forEach(o=>{r[o]=!1}),this._previousPanelClass=e,e&&e.length&&(e.split(" ").forEach(o=>{r[o]=!0}),this._elementRef.nativeElement.className=""),this._classList=r}_previousPanelClass;get classList(){return this.panelClass}set classList(e){this.panelClass=e}closed=new U;close=this.closed;panelId=d(Ie).getId("mat-menu-panel-");constructor(){let e=d(pj);this.overlayPanelClass=e.overlayPanelClass||"",this._xPosition=e.xPosition,this._yPosition=e.yPosition,this.backdropClass=e.backdropClass,this.overlapTrigger=e.overlapTrigger,this.hasBackdrop=e.hasBackdrop}ngOnInit(){this.setPositionClasses()}ngAfterContentInit(){this._updateDirectDescendants(),this._keyManager=new xr(this._directDescendantItems).withWrap().withTypeAhead().withHomeAndEnd(),this._keyManager.tabOut.subscribe(()=>this.closed.emit("tab")),this._directDescendantItems.changes.pipe(et(this._directDescendantItems),gt(e=>mn(...e.map(i=>i._focused)))).subscribe(e=>this._keyManager.updateActiveItem(e)),this._directDescendantItems.changes.subscribe(e=>{let i=this._keyManager;if(this._panelAnimationState==="enter"&&i.activeItem?._hasFocus()){let r=e.toArray(),o=Math.max(0,Math.min(r.length-1,i.activeItemIndex||0));r[o]&&!r[o].disabled?i.setActiveItem(o):i.setNextItemActive()}})}ngOnDestroy(){this._keyManager?.destroy(),this._directDescendantItems.destroy(),this.closed.complete(),this._firstItemFocusRef?.destroy(),clearTimeout(this._exitFallbackTimeout)}_hovered(){return this._directDescendantItems.changes.pipe(et(this._directDescendantItems),gt(i=>mn(...i.map(r=>r._hovered))))}addItem(e){}removeItem(e){}_handleKeydown(e){let i=e.keyCode,r=this._keyManager;switch(i){case 27:Ct(e)||(e.preventDefault(),this.closed.emit("keydown"));break;case 37:this.parentMenu&&this.direction==="ltr"&&this.closed.emit("keydown");break;case 39:this.parentMenu&&this.direction==="rtl"&&this.closed.emit("keydown");break;default:(i===38||i===40)&&r.setFocusOrigin("keyboard"),r.onKeydown(e);return}}focusFirstItem(e="program"){this._firstItemFocusRef?.destroy(),this._firstItemFocusRef=bt(()=>{let i=this._resolvePanel();if(!i||!i.contains(document.activeElement)){let r=this._keyManager;r.setFocusOrigin(e).setFirstItemActive(),!r.activeItem&&i&&i.focus()}},{injector:this._injector})}resetActiveItem(){this._keyManager.setActiveItem(-1)}setElevation(e){}setPositionClasses(e=this.xPosition,i=this.yPosition){this._classList=Z(b({},this._classList),{"mat-menu-before":e==="before","mat-menu-after":e==="after","mat-menu-above":i==="above","mat-menu-below":i==="below"}),this._changeDetectorRef.markForCheck()}_onAnimationDone(e){let i=e===nh;(i||e===oy)&&(i&&(clearTimeout(this._exitFallbackTimeout),this._exitFallbackTimeout=void 0),this._animationDone.next(i?"void":"enter"),this._isAnimating.set(!1))}_onAnimationStart(e){(e===oy||e===nh)&&this._isAnimating.set(!0)}_setIsOpen(e){if(this._panelAnimationState=e?"enter":"void",e){if(this._keyManager.activeItemIndex===0){let i=this._resolvePanel();i&&(i.scrollTop=0)}}else this._animationsDisabled||(this._exitFallbackTimeout=setTimeout(()=>this._onAnimationDone(nh),200));this._animationsDisabled&&setTimeout(()=>{this._onAnimationDone(e?oy:nh)}),this._changeDetectorRef.markForCheck()}_updateDirectDescendants(){this._allItems.changes.pipe(et(this._allItems)).subscribe(e=>{this._directDescendantItems.reset(e.filter(i=>i._parentMenu===this)),this._directDescendantItems.notifyOnChanges()})}_resolvePanel(){let e=null;return this._directDescendantItems.length&&(e=this._directDescendantItems.first._getHostElement().closest('[role="menu"]')),e}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=E({type:t,selectors:[["mat-menu"]],contentQueries:function(i,r,o){if(i&1&&yt(o,hj,5)(o,dd,5)(o,dd,4),i&2){let s;$(s=G())&&(r.lazyContent=s.first),$(s=G())&&(r._allItems=s),$(s=G())&&(r.items=s)}},viewQuery:function(i,r){if(i&1&&Ve(vt,5),i&2){let o;$(o=G())&&(r.templateRef=o.first)}},hostVars:3,hostBindings:function(i,r){i&2&&q("aria-label",null)("aria-labelledby",null)("aria-describedby",null)},inputs:{backdropClass:"backdropClass",ariaLabel:[0,"aria-label","ariaLabel"],ariaLabelledby:[0,"aria-labelledby","ariaLabelledby"],ariaDescribedby:[0,"aria-describedby","ariaDescribedby"],xPosition:"xPosition",yPosition:"yPosition",overlapTrigger:[2,"overlapTrigger","overlapTrigger",B],hasBackdrop:[2,"hasBackdrop","hasBackdrop",e=>e==null?null:B(e)],panelClass:[0,"class","panelClass"],classList:"classList"},outputs:{closed:"closed",close:"close"},exportAs:["matMenu"],features:[Ee([{provide:sy,useExisting:t}])],ngContentSelectors:mj,decls:1,vars:0,consts:[["tabindex","-1","role","menu",1,"mat-mdc-menu-panel",3,"click","animationstart","animationend","animationcancel","id"],[1,"mat-mdc-menu-content"]],template:function(i,r){i&1&&(Ce(),ta(0,fj,3,12,"ng-template"))},styles:[`mat-menu {
  display: none;
}

.mat-mdc-menu-content {
  margin: 0;
  padding: 8px 0;
  outline: 0;
}
.mat-mdc-menu-content,
.mat-mdc-menu-content .mat-mdc-menu-item .mat-mdc-menu-item-text {
  -moz-osx-font-smoothing: grayscale;
  -webkit-font-smoothing: antialiased;
  flex: 1;
  white-space: normal;
  font-family: var(--mat-menu-item-label-text-font, var(--mat-sys-label-large-font));
  line-height: var(--mat-menu-item-label-text-line-height, var(--mat-sys-label-large-line-height));
  font-size: var(--mat-menu-item-label-text-size, var(--mat-sys-label-large-size));
  letter-spacing: var(--mat-menu-item-label-text-tracking, var(--mat-sys-label-large-tracking));
  font-weight: var(--mat-menu-item-label-text-weight, var(--mat-sys-label-large-weight));
}

@keyframes _mat-menu-enter {
  from {
    opacity: 0;
    transform: scale(0.8);
  }
  to {
    opacity: 1;
    transform: none;
  }
}
@keyframes _mat-menu-exit {
  from {
    opacity: 1;
  }
  to {
    opacity: 0;
  }
}
.mat-mdc-menu-panel {
  min-width: 112px;
  max-width: 280px;
  overflow: auto;
  box-sizing: border-box;
  outline: 0;
  animation: _mat-menu-enter 120ms cubic-bezier(0, 0, 0.2, 1);
  border-radius: var(--mat-menu-container-shape, var(--mat-sys-corner-extra-small));
  background-color: var(--mat-menu-container-color, var(--mat-sys-surface-container));
  box-shadow: var(--mat-menu-container-elevation-shadow, 0px 3px 1px -2px rgba(0, 0, 0, 0.2), 0px 2px 2px 0px rgba(0, 0, 0, 0.14), 0px 1px 5px 0px rgba(0, 0, 0, 0.12));
  will-change: transform, opacity;
}
.mat-mdc-menu-panel.mat-menu-panel-exit-animation {
  animation: _mat-menu-exit 100ms 25ms linear forwards;
}
.mat-mdc-menu-panel.mat-menu-panel-animations-disabled {
  animation: none;
}
.mat-mdc-menu-panel.mat-menu-panel-animating {
  pointer-events: none;
}
.mat-mdc-menu-panel.mat-menu-panel-animating:has(.mat-mdc-menu-content:empty) {
  display: none;
}
@media (forced-colors: active) {
  .mat-mdc-menu-panel {
    outline: solid 1px;
  }
}
.mat-mdc-menu-panel .mat-divider {
  border-top-color: var(--mat-menu-divider-color, var(--mat-sys-surface-variant));
  margin-bottom: var(--mat-menu-divider-bottom-spacing, 8px);
  margin-top: var(--mat-menu-divider-top-spacing, 8px);
}

.mat-mdc-menu-item {
  display: flex;
  position: relative;
  align-items: center;
  justify-content: flex-start;
  overflow: hidden;
  padding: 0;
  cursor: pointer;
  width: 100%;
  text-align: left;
  box-sizing: border-box;
  color: inherit;
  font-size: inherit;
  background: none;
  text-decoration: none;
  margin: 0;
  min-height: 48px;
  padding-left: var(--mat-menu-item-leading-spacing, 12px);
  padding-right: var(--mat-menu-item-trailing-spacing, 12px);
  -webkit-user-select: none;
  user-select: none;
  cursor: pointer;
  outline: none;
  border: none;
  -webkit-tap-highlight-color: transparent;
}
.mat-mdc-menu-item::-moz-focus-inner {
  border: 0;
}
[dir=rtl] .mat-mdc-menu-item {
  padding-left: var(--mat-menu-item-trailing-spacing, 12px);
  padding-right: var(--mat-menu-item-leading-spacing, 12px);
}
.mat-mdc-menu-item:has(.material-icons, mat-icon, [matButtonIcon]) {
  padding-left: var(--mat-menu-item-with-icon-leading-spacing, 12px);
  padding-right: var(--mat-menu-item-with-icon-trailing-spacing, 12px);
}
[dir=rtl] .mat-mdc-menu-item:has(.material-icons, mat-icon, [matButtonIcon]) {
  padding-left: var(--mat-menu-item-with-icon-trailing-spacing, 12px);
  padding-right: var(--mat-menu-item-with-icon-leading-spacing, 12px);
}
.mat-mdc-menu-item, .mat-mdc-menu-item:visited, .mat-mdc-menu-item:link {
  color: var(--mat-menu-item-label-text-color, var(--mat-sys-on-surface));
}
.mat-mdc-menu-item .mat-icon-no-color,
.mat-mdc-menu-item .mat-mdc-menu-submenu-icon {
  color: var(--mat-menu-item-icon-color, var(--mat-sys-on-surface-variant));
}
.mat-mdc-menu-item[disabled] {
  cursor: default;
  opacity: 0.38;
}
.mat-mdc-menu-item[disabled]::after {
  display: block;
  position: absolute;
  content: "";
  top: 0;
  left: 0;
  bottom: 0;
  right: 0;
}
.mat-mdc-menu-item:focus {
  outline: 0;
}
.mat-mdc-menu-item .mat-icon {
  flex-shrink: 0;
  margin-right: var(--mat-menu-item-spacing, 12px);
  height: var(--mat-menu-item-icon-size, 24px);
  width: var(--mat-menu-item-icon-size, 24px);
}
[dir=rtl] .mat-mdc-menu-item {
  text-align: right;
}
[dir=rtl] .mat-mdc-menu-item .mat-icon {
  margin-right: 0;
  margin-left: var(--mat-menu-item-spacing, 12px);
}
.mat-mdc-menu-item:not([disabled]):hover {
  background-color: var(--mat-menu-item-hover-state-layer-color, color-mix(in srgb, var(--mat-sys-on-surface) calc(var(--mat-sys-hover-state-layer-opacity) * 100%), transparent));
}
.mat-mdc-menu-item:not([disabled]).cdk-program-focused, .mat-mdc-menu-item:not([disabled]).cdk-keyboard-focused, .mat-mdc-menu-item:not([disabled]).mat-mdc-menu-item-highlighted {
  background-color: var(--mat-menu-item-focus-state-layer-color, color-mix(in srgb, var(--mat-sys-on-surface) calc(var(--mat-sys-focus-state-layer-opacity) * 100%), transparent));
}
@media (forced-colors: active) {
  .mat-mdc-menu-item {
    margin-top: 1px;
  }
}

.mat-mdc-menu-submenu-icon {
  width: var(--mat-menu-item-icon-size, 24px);
  height: 10px;
  fill: currentColor;
  padding-left: var(--mat-menu-item-spacing, 12px);
}
[dir=rtl] .mat-mdc-menu-submenu-icon {
  padding-right: var(--mat-menu-item-spacing, 12px);
  padding-left: 0;
}
[dir=rtl] .mat-mdc-menu-submenu-icon polygon {
  transform: scaleX(-1);
  transform-origin: center;
}
@media (forced-colors: active) {
  .mat-mdc-menu-submenu-icon {
    fill: CanvasText;
  }
}

.mat-mdc-menu-item .mat-mdc-menu-ripple {
  top: 0;
  left: 0;
  right: 0;
  bottom: 0;
  position: absolute;
  pointer-events: none;
}
`],encapsulation:2,changeDetection:0})}return t})(),gj=new y("mat-menu-scroll-strategy",{providedIn:"root",factory:()=>{let t=d(X);return()=>lo(t)}});var Ga=new WeakMap,_j=(()=>{class t{_canHaveBackdrop;_element=d(L);_viewContainerRef=d(Vt);_menuItemInstance=d(dd,{optional:!0,self:!0});_dir=d(Xt,{optional:!0});_focusMonitor=d(Tn);_ngZone=d(H);_injector=d(X);_scrollStrategy=d(gj);_changeDetectorRef=d(ve);_animationsDisabled=ke();_portal;_overlayRef=null;_menuOpen=!1;_closingActionsSubscription=ge.EMPTY;_menuCloseSubscription=ge.EMPTY;_pendingRemoval;_parentMaterialMenu;_parentInnerPadding;_openedBy=void 0;get _menu(){return this._menuInternal}set _menu(e){e!==this._menuInternal&&(this._menuInternal=e,this._menuCloseSubscription.unsubscribe(),e&&(this._parentMaterialMenu,this._menuCloseSubscription=e.close.subscribe(i=>{this._destroyMenu(i),(i==="click"||i==="tab")&&this._parentMaterialMenu&&this._parentMaterialMenu.closed.emit(i)})),this._menuItemInstance?._setTriggersSubmenu(this._triggersSubmenu()))}_menuInternal=null;constructor(e){this._canHaveBackdrop=e;let i=d(sy,{optional:!0});this._parentMaterialMenu=i instanceof po?i:void 0}ngOnDestroy(){this._menu&&this._ownsMenu(this._menu)&&Ga.delete(this._menu),this._pendingRemoval?.unsubscribe(),this._menuCloseSubscription.unsubscribe(),this._closingActionsSubscription.unsubscribe(),this._overlayRef&&(this._overlayRef.dispose(),this._overlayRef=null)}get menuOpen(){return this._menuOpen}get dir(){return this._dir&&this._dir.value==="rtl"?"rtl":"ltr"}_triggersSubmenu(){return!!(this._menuItemInstance&&this._parentMaterialMenu&&this._menu)}_closeMenu(){this._menu?.close.emit()}_openMenu(e){if(this._triggerIsAriaDisabled())return;let i=this._menu;if(this._menuOpen||!i)return;this._pendingRemoval?.unsubscribe();let r=Ga.get(i);Ga.set(i,this),r&&r!==this&&r._closeMenu();let o=this._createOverlay(i),s=o.getConfig(),a=s.positionStrategy;this._setPosition(i,a),this._canHaveBackdrop?s.hasBackdrop=i.hasBackdrop==null?!this._triggersSubmenu():i.hasBackdrop:s.hasBackdrop=i.hasBackdrop??!1,o.hasAttached()||(o.attach(this._getPortal(i)),i.lazyContent?.attach(this.menuData)),this._closingActionsSubscription=this._menuClosingActions().subscribe(()=>this._closeMenu()),i.parentMenu=this._triggersSubmenu()?this._parentMaterialMenu:void 0,i.direction=this.dir,e&&i.focusFirstItem(this._openedBy||"program"),this._setIsMenuOpen(!0),i instanceof po&&(i._setIsOpen(!0),i._directDescendantItems.changes.pipe(me(i.close)).subscribe(()=>{a.withLockedPosition(!1).reapplyLastPosition(),a.withLockedPosition(!0)}))}focus(e,i){this._focusMonitor&&e?this._focusMonitor.focusVia(this._element,e,i):this._element.nativeElement.focus(i)}_destroyMenu(e){let i=this._overlayRef,r=this._menu;!i||!this.menuOpen||(this._closingActionsSubscription.unsubscribe(),this._pendingRemoval?.unsubscribe(),r instanceof po&&this._ownsMenu(r)?(this._pendingRemoval=r._animationDone.pipe(ut(1)).subscribe(()=>{i.detach(),Ga.has(r)||r.lazyContent?.detach()}),r._setIsOpen(!1)):(i.detach(),r?.lazyContent?.detach()),r&&this._ownsMenu(r)&&Ga.delete(r),this.restoreFocus&&(e==="keydown"||!this._openedBy||!this._triggersSubmenu())&&this.focus(this._openedBy),this._openedBy=void 0,this._setIsMenuOpen(!1))}_setIsMenuOpen(e){e!==this._menuOpen&&(this._menuOpen=e,this._menuOpen?this.menuOpened.emit():this.menuClosed.emit(),this._triggersSubmenu()&&this._menuItemInstance._setHighlighted(e),this._changeDetectorRef.markForCheck())}_createOverlay(e){if(!this._overlayRef){let i=this._getOverlayConfig(e);this._subscribeToPositions(e,i.positionStrategy),this._overlayRef=Ji(this._injector,i),this._overlayRef.keydownEvents().subscribe(r=>{this._menu instanceof po&&this._menu._handleKeydown(r)})}return this._overlayRef}_getOverlayConfig(e){return new wi({positionStrategy:Na(this._injector,this._getOverlayOrigin()).withLockedPosition().withGrowAfterOpen().withTransformOriginOn(".mat-menu-panel, .mat-mdc-menu-panel"),backdropClass:e.backdropClass||"cdk-overlay-transparent-backdrop",panelClass:e.overlayPanelClass,scrollStrategy:this._scrollStrategy(),direction:this._dir||"ltr",disableAnimations:this._animationsDisabled})}_subscribeToPositions(e,i){e.setPositionClasses&&i.positionChanges.subscribe(r=>{this._ngZone.run(()=>{let o=r.connectionPair.overlayX==="start"?"after":"before",s=r.connectionPair.overlayY==="top"?"below":"above";e.setPositionClasses(o,s)})})}_setPosition(e,i){let[r,o]=e.xPosition==="before"?["end","start"]:["start","end"],[s,a]=e.yPosition==="above"?["bottom","top"]:["top","bottom"],[c,l]=[s,a],[u,m]=[r,o],g=0;if(this._triggersSubmenu()){if(m=r=e.xPosition==="before"?"start":"end",o=u=r==="end"?"start":"end",this._parentMaterialMenu){if(this._parentInnerPadding==null){let v=this._parentMaterialMenu.items.first;this._parentInnerPadding=v?v._getHostElement().offsetTop:0}g=s==="bottom"?this._parentInnerPadding:-this._parentInnerPadding}}else e.overlapTrigger||(c=s==="top"?"bottom":"top",l=a==="top"?"bottom":"top");i.withPositions([{originX:r,originY:c,overlayX:u,overlayY:s,offsetY:g},{originX:o,originY:c,overlayX:m,overlayY:s,offsetY:g},{originX:r,originY:l,overlayX:u,overlayY:a,offsetY:-g},{originX:o,originY:l,overlayX:m,overlayY:a,offsetY:-g}])}_menuClosingActions(){let e=this._getOutsideClickStream(this._overlayRef),i=this._overlayRef.detachments(),r=this._parentMaterialMenu?this._parentMaterialMenu.closed:W(),o=this._parentMaterialMenu?this._parentMaterialMenu._hovered().pipe(De(s=>this._menuOpen&&s!==this._menuItemInstance)):W();return mn(e,r,o,i)}_getPortal(e){return(!this._portal||this._portal.templateRef!==e.templateRef)&&(this._portal=new Un(e.templateRef,this._viewContainerRef)),this._portal}_ownsMenu(e){return Ga.get(e)===this}_triggerIsAriaDisabled(){return B(this._element.nativeElement.getAttribute("aria-disabled"))}static \u0275fac=function(i){fm()};static \u0275dir=A({type:t})}return t})(),ih=(()=>{class t extends _j{_cleanupTouchstart;_hoverSubscription=ge.EMPTY;get _deprecatedMatMenuTriggerFor(){return this.menu}set _deprecatedMatMenuTriggerFor(e){this.menu=e}get menu(){return this._menu}set menu(e){this._menu=e}menuData;restoreFocus=!0;menuOpened=new U;onMenuOpen=this.menuOpened;menuClosed=new U;onMenuClose=this.menuClosed;constructor(){super(!0);let e=d(xe);this._cleanupTouchstart=e.listen(this._element.nativeElement,"touchstart",i=>{fs(i)||(this._openedBy="touch")},{passive:!0})}triggersSubmenu(){return super._triggersSubmenu()}toggleMenu(){return this.menuOpen?this.closeMenu():this.openMenu()}openMenu(){this._openMenu(!0)}closeMenu(){this._closeMenu()}updatePosition(){this._overlayRef?.updatePosition()}ngAfterContentInit(){this._handleHover()}ngOnDestroy(){super.ngOnDestroy(),this._cleanupTouchstart(),this._hoverSubscription.unsubscribe()}_getOverlayOrigin(){return this._element}_getOutsideClickStream(e){return e.backdropClick()}_handleMousedown(e){ms(e)||(this._openedBy=e.button===0?"mouse":void 0,this.triggersSubmenu()&&e.preventDefault())}_handleKeydown(e){let i=e.keyCode;(i===13||i===32)&&(this._openedBy="keyboard"),this.triggersSubmenu()&&(i===39&&this.dir==="ltr"||i===37&&this.dir==="rtl")&&(this._openedBy="keyboard",this.openMenu())}_handleClick(e){this.triggersSubmenu()?(e.stopPropagation(),this.openMenu()):this.toggleMenu()}_handleHover(){this.triggersSubmenu()&&this._parentMaterialMenu&&(this._hoverSubscription=this._parentMaterialMenu._hovered().subscribe(e=>{e===this._menuItemInstance&&!e.disabled&&this._parentMaterialMenu?._panelAnimationState!=="void"&&(this._openedBy="mouse",this._openMenu(!1))}))}static \u0275fac=function(i){return new(i||t)};static \u0275dir=A({type:t,selectors:[["","mat-menu-trigger-for",""],["","matMenuTriggerFor",""]],hostAttrs:[1,"mat-mdc-menu-trigger"],hostVars:3,hostBindings:function(i,r){i&1&&T("click",function(s){return r._handleClick(s)})("mousedown",function(s){return r._handleMousedown(s)})("keydown",function(s){return r._handleKeydown(s)}),i&2&&q("aria-haspopup",r.menu?"menu":null)("aria-expanded",r.menuOpen)("aria-controls",r.menuOpen?r.menu==null?null:r.menu.panelId:null)},inputs:{_deprecatedMatMenuTriggerFor:[0,"mat-menu-trigger-for","_deprecatedMatMenuTriggerFor"],menu:[0,"matMenuTriggerFor","menu"],menuData:[0,"matMenuTriggerData","menuData"],restoreFocus:[0,"matMenuTriggerRestoreFocus","restoreFocus"]},outputs:{menuOpened:"menuOpened",onMenuOpen:"onMenuOpen",menuClosed:"menuClosed",onMenuClose:"onMenuClose"},exportAs:["matMenuTrigger"],features:[fe]})}return t})();var rh=class{genres=[];sort="nameAsc";pageIndex=1;pageSize=10;search=""};var ay=class{_box;_destroyed=new I;_resizeSubject=new I;_resizeObserver;_elementObservables=new Map;constructor(n){this._box=n,typeof ResizeObserver<"u"&&(this._resizeObserver=new ResizeObserver(e=>this._resizeSubject.next(e)))}observe(n){return this._elementObservables.has(n)||this._elementObservables.set(n,new ce(e=>{let i=this._resizeSubject.subscribe(e);return this._resizeObserver?.observe(n,{box:this._box}),()=>{this._resizeObserver?.unobserve(n),i.unsubscribe(),this._elementObservables.delete(n)}}).pipe(De(e=>e.some(i=>i.target===n)),eu({bufferSize:1,refCount:!0}),me(this._destroyed))),this._elementObservables.get(n)}destroy(){this._destroyed.next(),this._destroyed.complete(),this._resizeSubject.complete(),this._elementObservables.clear()}},LI=(()=>{class t{_cleanupErrorListener;_observers=new Map;_ngZone=d(H);constructor(){typeof ResizeObserver<"u"}ngOnDestroy(){for(let[,e]of this._observers)e.destroy();this._observers.clear(),this._cleanupErrorListener?.()}observe(e,i){let r=i?.box||"content-box";return this._observers.has(r)||this._observers.set(r,new ay(r)),this._observers.get(r).observe(e)}static \u0275fac=function(i){return new(i||t)};static \u0275prov=C({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();var vj=["notch"],bj=["matFormFieldNotchedOutline",""],yj=["*"],jI=["iconPrefixContainer"],BI=["textPrefixContainer"],VI=["iconSuffixContainer"],HI=["textSuffixContainer"],xj=["textField"],Cj=["*",[["mat-label"]],[["","matPrefix",""],["","matIconPrefix",""]],[["","matTextPrefix",""]],[["","matTextSuffix",""]],[["","matSuffix",""],["","matIconSuffix",""]],[["mat-error"],["","matError",""]],[["mat-hint",3,"align","end"]],[["mat-hint","align","end"]]],wj=["*","mat-label","[matPrefix], [matIconPrefix]","[matTextPrefix]","[matTextSuffix]","[matSuffix], [matIconSuffix]","mat-error, [matError]","mat-hint:not([align='end'])","mat-hint[align='end']"];function Dj(t,n){t&1&&O(0,"span",21)}function Ej(t,n){if(t&1&&(f(0,"label",20),re(1,1),N(2,Dj,1,0,"span",21),h()),t&2){let e=S(2);D("floating",e._shouldLabelFloat())("monitorResize",e._hasOutline())("id",e._labelId),q("for",e._control.disableAutomaticLabeling?null:e._control.id),p(2),P(!e.hideRequiredMarker&&e._control.required?2:-1)}}function Sj(t,n){if(t&1&&N(0,Ej,3,5,"label",20),t&2){let e=S();P(e._hasFloatingLabel()?0:-1)}}function Ij(t,n){t&1&&O(0,"div",7)}function kj(t,n){}function Mj(t,n){if(t&1&&$e(0,kj,0,0,"ng-template",13),t&2){S(2);let e=Re(1);D("ngTemplateOutlet",e)}}function Tj(t,n){if(t&1&&(f(0,"div",9),N(1,Mj,1,1,null,13),h()),t&2){let e=S();D("matFormFieldNotchedOutlineOpen",e._shouldLabelFloat()),p(),P(e._forceDisplayInfixLabel()?-1:1)}}function Aj(t,n){t&1&&(f(0,"div",10,2),re(2,2),h())}function Rj(t,n){t&1&&(f(0,"div",11,3),re(2,3),h())}function Oj(t,n){}function Nj(t,n){if(t&1&&$e(0,Oj,0,0,"ng-template",13),t&2){S();let e=Re(1);D("ngTemplateOutlet",e)}}function Pj(t,n){t&1&&(f(0,"div",14,4),re(2,4),h())}function Fj(t,n){t&1&&(f(0,"div",15,5),re(2,5),h())}function Lj(t,n){t&1&&O(0,"div",16)}function jj(t,n){t&1&&(f(0,"div",18),re(1,6),h())}function Bj(t,n){if(t&1&&(f(0,"mat-hint",22),_(1),h()),t&2){let e=S(2);D("id",e._hintLabelId),p(),V(e.hintLabel)}}function Vj(t,n){if(t&1&&(f(0,"div",19),N(1,Bj,2,2,"mat-hint",22),re(2,7),O(3,"div",23),re(4,8),h()),t&2){let e=S();p(),P(e.hintLabel?1:-1)}}var Ir=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275dir=A({type:t,selectors:[["mat-label"]]})}return t})(),YI=new y("MatError"),ly=(()=>{class t{id=d(Ie).getId("mat-mdc-error-");constructor(){}static \u0275fac=function(i){return new(i||t)};static \u0275dir=A({type:t,selectors:[["mat-error"],["","matError",""]],hostAttrs:[1,"mat-mdc-form-field-error","mat-mdc-form-field-bottom-align"],hostVars:1,hostBindings:function(i,r){i&2&&mt("id",r.id)},inputs:{id:"id"},features:[Ee([{provide:YI,useExisting:t}])]})}return t})(),cy=(()=>{class t{align="start";id=d(Ie).getId("mat-mdc-hint-");static \u0275fac=function(i){return new(i||t)};static \u0275dir=A({type:t,selectors:[["mat-hint"]],hostAttrs:[1,"mat-mdc-form-field-hint","mat-mdc-form-field-bottom-align"],hostVars:4,hostBindings:function(i,r){i&2&&(mt("id",r.id),q("align",null),z("mat-mdc-form-field-hint-end",r.align==="end"))},inputs:{align:"align",id:"id"}})}return t})(),Hj=new y("MatPrefix");var Uj=new y("MatSuffix");var ZI=new y("FloatingLabelParent"),UI=(()=>{class t{_elementRef=d(L);get floating(){return this._floating}set floating(e){this._floating=e,this.monitorResize&&this._handleResize()}_floating=!1;get monitorResize(){return this._monitorResize}set monitorResize(e){this._monitorResize=e,this._monitorResize?this._subscribeToResize():this._resizeSubscription.unsubscribe()}_monitorResize=!1;_resizeObserver=d(LI);_ngZone=d(H);_parent=d(ZI);_resizeSubscription=new ge;constructor(){}ngOnDestroy(){this._resizeSubscription.unsubscribe()}getWidth(){return zj(this._elementRef.nativeElement)}get element(){return this._elementRef.nativeElement}_handleResize(){setTimeout(()=>this._parent._handleLabelResized())}_subscribeToResize(){this._resizeSubscription.unsubscribe(),this._ngZone.runOutsideAngular(()=>{this._resizeSubscription=this._resizeObserver.observe(this._elementRef.nativeElement,{box:"border-box"}).subscribe(()=>this._handleResize())})}static \u0275fac=function(i){return new(i||t)};static \u0275dir=A({type:t,selectors:[["label","matFormFieldFloatingLabel",""]],hostAttrs:[1,"mdc-floating-label","mat-mdc-floating-label"],hostVars:2,hostBindings:function(i,r){i&2&&z("mdc-floating-label--float-above",r.floating)},inputs:{floating:"floating",monitorResize:"monitorResize"}})}return t})();function zj(t){let n=t;if(n.offsetParent!==null)return n.scrollWidth;let e=n.cloneNode(!0);e.style.setProperty("position","absolute"),e.style.setProperty("transform","translate(-9999px, -9999px)"),document.documentElement.appendChild(e);let i=e.scrollWidth;return e.remove(),i}var zI="mdc-line-ripple--active",oh="mdc-line-ripple--deactivating",$I=(()=>{class t{_elementRef=d(L);_cleanupTransitionEnd;constructor(){let e=d(H),i=d(xe);e.runOutsideAngular(()=>{this._cleanupTransitionEnd=i.listen(this._elementRef.nativeElement,"transitionend",this._handleTransitionEnd)})}activate(){let e=this._elementRef.nativeElement.classList;e.remove(oh),e.add(zI)}deactivate(){this._elementRef.nativeElement.classList.add(oh)}_handleTransitionEnd=e=>{let i=this._elementRef.nativeElement.classList,r=i.contains(oh);e.propertyName==="opacity"&&r&&i.remove(zI,oh)};ngOnDestroy(){this._cleanupTransitionEnd()}static \u0275fac=function(i){return new(i||t)};static \u0275dir=A({type:t,selectors:[["div","matFormFieldLineRipple",""]],hostAttrs:[1,"mdc-line-ripple"]})}return t})(),GI=(()=>{class t{_elementRef=d(L);_ngZone=d(H);open=!1;_notch;ngAfterViewInit(){let e=this._elementRef.nativeElement,i=e.querySelector(".mdc-floating-label");i?(e.classList.add("mdc-notched-outline--upgraded"),typeof requestAnimationFrame=="function"&&(i.style.transitionDuration="0s",this._ngZone.runOutsideAngular(()=>{requestAnimationFrame(()=>i.style.transitionDuration="")}))):e.classList.add("mdc-notched-outline--no-label")}_setNotchWidth(e){let i=this._notch.nativeElement;!this.open||!e?i.style.width="":i.style.width=`calc(${e}px * var(--mat-mdc-form-field-floating-label-scale, 0.75) + 9px)`}_setMaxWidth(e){this._notch.nativeElement.style.setProperty("--mat-form-field-notch-max-width",`calc(100% - ${e}px)`)}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=E({type:t,selectors:[["div","matFormFieldNotchedOutline",""]],viewQuery:function(i,r){if(i&1&&Ve(vj,5),i&2){let o;$(o=G())&&(r._notch=o.first)}},hostAttrs:[1,"mdc-notched-outline"],hostVars:2,hostBindings:function(i,r){i&2&&z("mdc-notched-outline--notched",r.open)},inputs:{open:[0,"matFormFieldNotchedOutlineOpen","open"]},attrs:bj,ngContentSelectors:yj,decls:5,vars:0,consts:[["notch",""],[1,"mat-mdc-notch-piece","mdc-notched-outline__leading"],[1,"mat-mdc-notch-piece","mdc-notched-outline__notch"],[1,"mat-mdc-notch-piece","mdc-notched-outline__trailing"]],template:function(i,r){i&1&&(Ce(),Ht(0,"div",1),Fe(1,"div",2,0),re(3),Ge(),Ht(4,"div",3))},encapsulation:2,changeDetection:0})}return t})(),ud=(()=>{class t{value=null;stateChanges;id;placeholder;ngControl=null;focused=!1;empty=!1;shouldLabelFloat=!1;required=!1;disabled=!1;errorState=!1;controlType;autofilled;userAriaDescribedBy;disableAutomaticLabeling;describedByIds;static \u0275fac=function(i){return new(i||t)};static \u0275dir=A({type:t})}return t})();var md=new y("MatFormField"),$j=new y("MAT_FORM_FIELD_DEFAULT_OPTIONS"),WI="fill",Gj="auto",qI="fixed",Wj="translateY(-50%)",kr=(()=>{class t{_elementRef=d(L);_changeDetectorRef=d(ve);_platform=d(ye);_idGenerator=d(Ie);_ngZone=d(H);_defaults=d($j,{optional:!0});_currentDirection;_textField;_iconPrefixContainer;_textPrefixContainer;_iconSuffixContainer;_textSuffixContainer;_floatingLabel;_notchedOutline;_lineRipple;_iconPrefixContainerSignal=nl("iconPrefixContainer");_textPrefixContainerSignal=nl("textPrefixContainer");_iconSuffixContainerSignal=nl("iconSuffixContainer");_textSuffixContainerSignal=nl("textSuffixContainer");_prefixSuffixContainers=xt(()=>[this._iconPrefixContainerSignal(),this._textPrefixContainerSignal(),this._iconSuffixContainerSignal(),this._textSuffixContainerSignal()].map(e=>e?.nativeElement).filter(e=>e!==void 0));_formFieldControl;_prefixChildren;_suffixChildren;_errorChildren;_hintChildren;_labelChild=iD(Ir);get hideRequiredMarker(){return this._hideRequiredMarker}set hideRequiredMarker(e){this._hideRequiredMarker=Rt(e)}_hideRequiredMarker=!1;color="primary";get floatLabel(){return this._floatLabel||this._defaults?.floatLabel||Gj}set floatLabel(e){e!==this._floatLabel&&(this._floatLabel=e,this._changeDetectorRef.markForCheck())}_floatLabel;get appearance(){return this._appearanceSignal()}set appearance(e){let i=e||this._defaults?.appearance||WI;this._appearanceSignal.set(i)}_appearanceSignal=M(WI);get subscriptSizing(){return this._subscriptSizing||this._defaults?.subscriptSizing||qI}set subscriptSizing(e){this._subscriptSizing=e||this._defaults?.subscriptSizing||qI}_subscriptSizing=null;get hintLabel(){return this._hintLabel}set hintLabel(e){this._hintLabel=e,this._processHints()}_hintLabel="";_hasIconPrefix=!1;_hasTextPrefix=!1;_hasIconSuffix=!1;_hasTextSuffix=!1;_labelId=this._idGenerator.getId("mat-mdc-form-field-label-");_hintLabelId=this._idGenerator.getId("mat-mdc-hint-");_describedByIds;get _control(){return this._explicitFormFieldControl||this._formFieldControl}set _control(e){this._explicitFormFieldControl=e}_destroyed=new I;_isFocused=null;_explicitFormFieldControl;_previousControl=null;_previousControlValidatorFn=null;_stateChanges;_valueChanges;_describedByChanges;_outlineLabelOffsetResizeObserver=null;_animationsDisabled=ke();constructor(){let e=this._defaults,i=d(Xt);e&&(e.appearance&&(this.appearance=e.appearance),this._hideRequiredMarker=!!e?.hideRequiredMarker,e.color&&(this.color=e.color)),fr(()=>this._currentDirection=i.valueSignal()),this._syncOutlineLabelOffset()}ngAfterViewInit(){this._updateFocusState(),this._animationsDisabled||this._ngZone.runOutsideAngular(()=>{setTimeout(()=>{this._elementRef.nativeElement.classList.add("mat-form-field-animations-enabled")},300)}),this._changeDetectorRef.detectChanges()}ngAfterContentInit(){this._assertFormFieldControl(),this._initializeSubscript(),this._initializePrefixAndSuffix()}ngAfterContentChecked(){this._assertFormFieldControl(),this._control!==this._previousControl&&(this._initializeControl(this._previousControl),this._control.ngControl&&this._control.ngControl.control&&(this._previousControlValidatorFn=this._control.ngControl.control.validator),this._previousControl=this._control),this._control.ngControl&&this._control.ngControl.control&&this._control.ngControl.control.validator!==this._previousControlValidatorFn&&this._changeDetectorRef.markForCheck()}ngOnDestroy(){this._outlineLabelOffsetResizeObserver?.disconnect(),this._stateChanges?.unsubscribe(),this._valueChanges?.unsubscribe(),this._describedByChanges?.unsubscribe(),this._destroyed.next(),this._destroyed.complete()}getLabelId=xt(()=>this._hasFloatingLabel()?this._labelId:null);getConnectedOverlayOrigin(){return this._textField||this._elementRef}_animateAndLockLabel(){this._hasFloatingLabel()&&(this.floatLabel="always")}_initializeControl(e){let i=this._control,r="mat-mdc-form-field-type-";e&&this._elementRef.nativeElement.classList.remove(r+e.controlType),i.controlType&&this._elementRef.nativeElement.classList.add(r+i.controlType),this._stateChanges?.unsubscribe(),this._stateChanges=i.stateChanges.subscribe(()=>{this._updateFocusState(),this._changeDetectorRef.markForCheck()}),this._describedByChanges?.unsubscribe(),this._describedByChanges=i.stateChanges.pipe(et([void 0,void 0]),Q(()=>[i.errorState,i.userAriaDescribedBy]),Jd(),De(([[o,s],[a,c]])=>o!==a||s!==c)).subscribe(()=>this._syncDescribedByIds()),this._valueChanges?.unsubscribe(),i.ngControl&&i.ngControl.valueChanges&&(this._valueChanges=i.ngControl.valueChanges.pipe(me(this._destroyed)).subscribe(()=>this._changeDetectorRef.markForCheck()))}_checkPrefixAndSuffixTypes(){this._hasIconPrefix=!!this._prefixChildren.find(e=>!e._isText),this._hasTextPrefix=!!this._prefixChildren.find(e=>e._isText),this._hasIconSuffix=!!this._suffixChildren.find(e=>!e._isText),this._hasTextSuffix=!!this._suffixChildren.find(e=>e._isText)}_initializePrefixAndSuffix(){this._checkPrefixAndSuffixTypes(),mn(this._prefixChildren.changes,this._suffixChildren.changes).subscribe(()=>{this._checkPrefixAndSuffixTypes(),this._changeDetectorRef.markForCheck()})}_initializeSubscript(){this._hintChildren.changes.subscribe(()=>{this._processHints(),this._changeDetectorRef.markForCheck()}),this._errorChildren.changes.subscribe(()=>{this._syncDescribedByIds(),this._changeDetectorRef.markForCheck()}),this._validateHints(),this._syncDescribedByIds()}_assertFormFieldControl(){this._control}_updateFocusState(){let e=this._control.focused;e&&!this._isFocused?(this._isFocused=!0,this._lineRipple?.activate()):!e&&(this._isFocused||this._isFocused===null)&&(this._isFocused=!1,this._lineRipple?.deactivate()),this._elementRef.nativeElement.classList.toggle("mat-focused",e),this._textField?.nativeElement.classList.toggle("mdc-text-field--focused",e)}_syncOutlineLabelOffset(){sD({earlyRead:()=>{if(this._appearanceSignal()!=="outline")return this._outlineLabelOffsetResizeObserver?.disconnect(),null;if(globalThis.ResizeObserver){this._outlineLabelOffsetResizeObserver||=new globalThis.ResizeObserver(()=>{this._writeOutlinedLabelStyles(this._getOutlinedLabelOffset())});for(let e of this._prefixSuffixContainers())this._outlineLabelOffsetResizeObserver.observe(e,{box:"border-box"})}return this._getOutlinedLabelOffset()},write:e=>this._writeOutlinedLabelStyles(e())})}_shouldAlwaysFloat(){return this.floatLabel==="always"}_hasOutline(){return this.appearance==="outline"}_forceDisplayInfixLabel(){return!this._platform.isBrowser&&this._prefixChildren.length&&!this._shouldLabelFloat()}_hasFloatingLabel=xt(()=>!!this._labelChild());_shouldLabelFloat(){return this._hasFloatingLabel()?this._control.shouldLabelFloat||this._shouldAlwaysFloat():!1}_shouldForward(e){let i=this._control?this._control.ngControl:null;return i&&i[e]}_getSubscriptMessageType(){return this._errorChildren&&this._errorChildren.length>0&&this._control.errorState?"error":"hint"}_handleLabelResized(){this._refreshOutlineNotchWidth()}_refreshOutlineNotchWidth(){!this._hasOutline()||!this._floatingLabel||!this._shouldLabelFloat()?this._notchedOutline?._setNotchWidth(0):this._notchedOutline?._setNotchWidth(this._floatingLabel.getWidth())}_processHints(){this._validateHints(),this._syncDescribedByIds()}_validateHints(){this._hintChildren}_syncDescribedByIds(){if(this._control){let e=[];if(this._control.userAriaDescribedBy&&typeof this._control.userAriaDescribedBy=="string"&&e.push(...this._control.userAriaDescribedBy.split(" ")),this._getSubscriptMessageType()==="hint"){let o=this._hintChildren?this._hintChildren.find(a=>a.align==="start"):null,s=this._hintChildren?this._hintChildren.find(a=>a.align==="end"):null;o?e.push(o.id):this._hintLabel&&e.push(this._hintLabelId),s&&e.push(s.id)}else this._errorChildren&&e.push(...this._errorChildren.map(o=>o.id));let i=this._control.describedByIds,r;if(i){let o=this._describedByIds||e;r=e.concat(i.filter(s=>s&&!o.includes(s)))}else r=e;this._control.setDescribedByIds(r),this._describedByIds=e}}_getOutlinedLabelOffset(){if(!this._hasOutline()||!this._floatingLabel)return null;if(!this._iconPrefixContainer&&!this._textPrefixContainer)return["",null];if(!this._isAttachedToDom())return null;let e=this._iconPrefixContainer?.nativeElement,i=this._textPrefixContainer?.nativeElement,r=this._iconSuffixContainer?.nativeElement,o=this._textSuffixContainer?.nativeElement,s=e?.getBoundingClientRect().width??0,a=i?.getBoundingClientRect().width??0,c=r?.getBoundingClientRect().width??0,l=o?.getBoundingClientRect().width??0,u=this._currentDirection==="rtl"?"-1":"1",m=`${s+a}px`,v=`calc(${u} * (${m} + var(--mat-mdc-form-field-label-offset-x, 0px)))`,x=`var(--mat-mdc-form-field-label-transform, ${Wj} translateX(${v}))`,F=s+a+c+l;return[x,F]}_writeOutlinedLabelStyles(e){if(e!==null){let[i,r]=e;this._floatingLabel&&(this._floatingLabel.element.style.transform=i),r!==null&&this._notchedOutline?._setMaxWidth(r)}}_isAttachedToDom(){let e=this._elementRef.nativeElement;if(e.getRootNode){let i=e.getRootNode();return i&&i!==e}return document.documentElement.contains(e)}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=E({type:t,selectors:[["mat-form-field"]],contentQueries:function(i,r,o){if(i&1&&(bm(o,r._labelChild,Ir,5),yt(o,ud,5)(o,Hj,5)(o,Uj,5)(o,YI,5)(o,cy,5)),i&2){xm();let s;$(s=G())&&(r._formFieldControl=s.first),$(s=G())&&(r._prefixChildren=s),$(s=G())&&(r._suffixChildren=s),$(s=G())&&(r._errorChildren=s),$(s=G())&&(r._hintChildren=s)}},viewQuery:function(i,r){if(i&1&&(ym(r._iconPrefixContainerSignal,jI,5)(r._textPrefixContainerSignal,BI,5)(r._iconSuffixContainerSignal,VI,5)(r._textSuffixContainerSignal,HI,5),Ve(xj,5)(jI,5)(BI,5)(VI,5)(HI,5)(UI,5)(GI,5)($I,5)),i&2){xm(4);let o;$(o=G())&&(r._textField=o.first),$(o=G())&&(r._iconPrefixContainer=o.first),$(o=G())&&(r._textPrefixContainer=o.first),$(o=G())&&(r._iconSuffixContainer=o.first),$(o=G())&&(r._textSuffixContainer=o.first),$(o=G())&&(r._floatingLabel=o.first),$(o=G())&&(r._notchedOutline=o.first),$(o=G())&&(r._lineRipple=o.first)}},hostAttrs:[1,"mat-mdc-form-field"],hostVars:38,hostBindings:function(i,r){i&2&&z("mat-mdc-form-field-label-always-float",r._shouldAlwaysFloat())("mat-mdc-form-field-has-icon-prefix",r._hasIconPrefix)("mat-mdc-form-field-has-icon-suffix",r._hasIconSuffix)("mat-form-field-invalid",r._control.errorState)("mat-form-field-disabled",r._control.disabled)("mat-form-field-autofilled",r._control.autofilled)("mat-form-field-appearance-fill",r.appearance=="fill")("mat-form-field-appearance-outline",r.appearance=="outline")("mat-form-field-hide-placeholder",r._hasFloatingLabel()&&!r._shouldLabelFloat())("mat-primary",r.color!=="accent"&&r.color!=="warn")("mat-accent",r.color==="accent")("mat-warn",r.color==="warn")("ng-untouched",r._shouldForward("untouched"))("ng-touched",r._shouldForward("touched"))("ng-pristine",r._shouldForward("pristine"))("ng-dirty",r._shouldForward("dirty"))("ng-valid",r._shouldForward("valid"))("ng-invalid",r._shouldForward("invalid"))("ng-pending",r._shouldForward("pending"))},inputs:{hideRequiredMarker:"hideRequiredMarker",color:"color",floatLabel:"floatLabel",appearance:"appearance",subscriptSizing:"subscriptSizing",hintLabel:"hintLabel"},exportAs:["matFormField"],features:[Ee([{provide:md,useExisting:t},{provide:ZI,useExisting:t}])],ngContentSelectors:wj,decls:18,vars:21,consts:[["labelTemplate",""],["textField",""],["iconPrefixContainer",""],["textPrefixContainer",""],["textSuffixContainer",""],["iconSuffixContainer",""],[1,"mat-mdc-text-field-wrapper","mdc-text-field",3,"click"],[1,"mat-mdc-form-field-focus-overlay"],[1,"mat-mdc-form-field-flex"],["matFormFieldNotchedOutline","",3,"matFormFieldNotchedOutlineOpen"],[1,"mat-mdc-form-field-icon-prefix"],[1,"mat-mdc-form-field-text-prefix"],[1,"mat-mdc-form-field-infix"],[3,"ngTemplateOutlet"],[1,"mat-mdc-form-field-text-suffix"],[1,"mat-mdc-form-field-icon-suffix"],["matFormFieldLineRipple",""],["aria-atomic","true","aria-live","polite",1,"mat-mdc-form-field-subscript-wrapper","mat-mdc-form-field-bottom-align"],[1,"mat-mdc-form-field-error-wrapper"],[1,"mat-mdc-form-field-hint-wrapper"],["matFormFieldFloatingLabel","",3,"floating","monitorResize","id"],["aria-hidden","true",1,"mat-mdc-form-field-required-marker","mdc-floating-label--required"],[3,"id"],[1,"mat-mdc-form-field-hint-spacer"]],template:function(i,r){if(i&1&&(Ce(Cj),$e(0,Sj,1,1,"ng-template",null,0,Xn),f(2,"div",6,1),T("click",function(s){return r._control.onContainerClick(s)}),N(4,Ij,1,0,"div",7),f(5,"div",8),N(6,Tj,2,2,"div",9),N(7,Aj,3,0,"div",10),N(8,Rj,3,0,"div",11),f(9,"div",12),N(10,Nj,1,1,null,13),re(11),h(),N(12,Pj,3,0,"div",14),N(13,Fj,3,0,"div",15),h(),N(14,Lj,1,0,"div",16),h(),f(15,"div",17),N(16,jj,2,0,"div",18)(17,Vj,5,1,"div",19),h()),i&2){let o;p(2),z("mdc-text-field--filled",!r._hasOutline())("mdc-text-field--outlined",r._hasOutline())("mdc-text-field--no-label",!r._hasFloatingLabel())("mdc-text-field--disabled",r._control.disabled)("mdc-text-field--invalid",r._control.errorState),p(2),P(!r._hasOutline()&&!r._control.disabled?4:-1),p(2),P(r._hasOutline()?6:-1),p(),P(r._hasIconPrefix?7:-1),p(),P(r._hasTextPrefix?8:-1),p(2),P(!r._hasOutline()||r._forceDisplayInfixLabel()?10:-1),p(2),P(r._hasTextSuffix?12:-1),p(),P(r._hasIconSuffix?13:-1),p(),P(r._hasOutline()?-1:14),p(),z("mat-mdc-form-field-subscript-dynamic-size",r.subscriptSizing==="dynamic");let s=r._getSubscriptMessageType();p(),P((o=s)==="error"?16:o==="hint"?17:-1)}},dependencies:[UI,GI,qi,$I,cy],styles:[`.mdc-text-field {
  display: inline-flex;
  align-items: baseline;
  padding: 0 16px;
  position: relative;
  box-sizing: border-box;
  overflow: hidden;
  will-change: opacity, transform, color;
  border-top-left-radius: 4px;
  border-top-right-radius: 4px;
  border-bottom-right-radius: 0;
  border-bottom-left-radius: 0;
}

.mdc-text-field__input {
  width: 100%;
  min-width: 0;
  border: none;
  border-radius: 0;
  background: none;
  padding: 0;
  -moz-appearance: none;
  -webkit-appearance: none;
  height: 28px;
}
.mdc-text-field__input::-webkit-calendar-picker-indicator, .mdc-text-field__input::-webkit-search-cancel-button {
  display: none;
}
.mdc-text-field__input::-ms-clear {
  display: none;
}
.mdc-text-field__input:focus {
  outline: none;
}
.mdc-text-field__input:invalid {
  box-shadow: none;
}
.mdc-text-field__input::placeholder {
  opacity: 0;
}
.mdc-text-field__input::-moz-placeholder {
  opacity: 0;
}
.mdc-text-field__input::-webkit-input-placeholder {
  opacity: 0;
}
.mdc-text-field__input:-ms-input-placeholder {
  opacity: 0;
}
.mdc-text-field--no-label .mdc-text-field__input::placeholder, .mdc-text-field--focused .mdc-text-field__input::placeholder {
  opacity: 1;
}
.mdc-text-field--no-label .mdc-text-field__input::-moz-placeholder, .mdc-text-field--focused .mdc-text-field__input::-moz-placeholder {
  opacity: 1;
}
.mdc-text-field--no-label .mdc-text-field__input::-webkit-input-placeholder, .mdc-text-field--focused .mdc-text-field__input::-webkit-input-placeholder {
  opacity: 1;
}
.mdc-text-field--no-label .mdc-text-field__input:-ms-input-placeholder, .mdc-text-field--focused .mdc-text-field__input:-ms-input-placeholder {
  opacity: 1;
}
.mdc-text-field--disabled:not(.mdc-text-field--no-label) .mdc-text-field__input.mat-mdc-input-disabled-interactive::placeholder {
  opacity: 0;
}
.mdc-text-field--disabled:not(.mdc-text-field--no-label) .mdc-text-field__input.mat-mdc-input-disabled-interactive::-moz-placeholder {
  opacity: 0;
}
.mdc-text-field--disabled:not(.mdc-text-field--no-label) .mdc-text-field__input.mat-mdc-input-disabled-interactive::-webkit-input-placeholder {
  opacity: 0;
}
.mdc-text-field--disabled:not(.mdc-text-field--no-label) .mdc-text-field__input.mat-mdc-input-disabled-interactive:-ms-input-placeholder {
  opacity: 0;
}
.mdc-text-field--outlined .mdc-text-field__input, .mdc-text-field--filled.mdc-text-field--no-label .mdc-text-field__input {
  height: 100%;
}
.mdc-text-field--outlined .mdc-text-field__input {
  display: flex;
  border: none !important;
  background-color: transparent;
}
.mdc-text-field--disabled .mdc-text-field__input {
  pointer-events: auto;
}
.mdc-text-field--filled:not(.mdc-text-field--disabled) .mdc-text-field__input {
  color: var(--mat-form-field-filled-input-text-color, var(--mat-sys-on-surface));
  caret-color: var(--mat-form-field-filled-caret-color, var(--mat-sys-primary));
}
.mdc-text-field--filled:not(.mdc-text-field--disabled) .mdc-text-field__input::placeholder {
  color: var(--mat-form-field-filled-input-text-placeholder-color, var(--mat-sys-on-surface-variant));
}
.mdc-text-field--filled:not(.mdc-text-field--disabled) .mdc-text-field__input::-moz-placeholder {
  color: var(--mat-form-field-filled-input-text-placeholder-color, var(--mat-sys-on-surface-variant));
}
.mdc-text-field--filled:not(.mdc-text-field--disabled) .mdc-text-field__input::-webkit-input-placeholder {
  color: var(--mat-form-field-filled-input-text-placeholder-color, var(--mat-sys-on-surface-variant));
}
.mdc-text-field--filled:not(.mdc-text-field--disabled) .mdc-text-field__input:-ms-input-placeholder {
  color: var(--mat-form-field-filled-input-text-placeholder-color, var(--mat-sys-on-surface-variant));
}
.mdc-text-field--outlined:not(.mdc-text-field--disabled) .mdc-text-field__input {
  color: var(--mat-form-field-outlined-input-text-color, var(--mat-sys-on-surface));
  caret-color: var(--mat-form-field-outlined-caret-color, var(--mat-sys-primary));
}
.mdc-text-field--outlined:not(.mdc-text-field--disabled) .mdc-text-field__input::placeholder {
  color: var(--mat-form-field-outlined-input-text-placeholder-color, var(--mat-sys-on-surface-variant));
}
.mdc-text-field--outlined:not(.mdc-text-field--disabled) .mdc-text-field__input::-moz-placeholder {
  color: var(--mat-form-field-outlined-input-text-placeholder-color, var(--mat-sys-on-surface-variant));
}
.mdc-text-field--outlined:not(.mdc-text-field--disabled) .mdc-text-field__input::-webkit-input-placeholder {
  color: var(--mat-form-field-outlined-input-text-placeholder-color, var(--mat-sys-on-surface-variant));
}
.mdc-text-field--outlined:not(.mdc-text-field--disabled) .mdc-text-field__input:-ms-input-placeholder {
  color: var(--mat-form-field-outlined-input-text-placeholder-color, var(--mat-sys-on-surface-variant));
}
.mdc-text-field--filled.mdc-text-field--invalid:not(.mdc-text-field--disabled) .mdc-text-field__input {
  caret-color: var(--mat-form-field-filled-error-caret-color, var(--mat-sys-error));
}
.mdc-text-field--outlined.mdc-text-field--invalid:not(.mdc-text-field--disabled) .mdc-text-field__input {
  caret-color: var(--mat-form-field-outlined-error-caret-color, var(--mat-sys-error));
}
.mdc-text-field--filled.mdc-text-field--disabled .mdc-text-field__input {
  color: var(--mat-form-field-filled-disabled-input-text-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
}
.mdc-text-field--outlined.mdc-text-field--disabled .mdc-text-field__input {
  color: var(--mat-form-field-outlined-disabled-input-text-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
}
@media (forced-colors: active) {
  .mdc-text-field--disabled .mdc-text-field__input {
    background-color: Window;
  }
}

.mdc-text-field--filled {
  height: 56px;
  border-bottom-right-radius: 0;
  border-bottom-left-radius: 0;
  border-top-left-radius: var(--mat-form-field-filled-container-shape, var(--mat-sys-corner-extra-small));
  border-top-right-radius: var(--mat-form-field-filled-container-shape, var(--mat-sys-corner-extra-small));
}
.mdc-text-field--filled:not(.mdc-text-field--disabled) {
  background-color: var(--mat-form-field-filled-container-color, var(--mat-sys-surface-variant));
}
.mdc-text-field--filled.mdc-text-field--disabled {
  background-color: var(--mat-form-field-filled-disabled-container-color, color-mix(in srgb, var(--mat-sys-on-surface) 4%, transparent));
}

.mdc-text-field--outlined {
  height: 56px;
  overflow: visible;
  padding-right: max(16px, var(--mat-form-field-outlined-container-shape, var(--mat-sys-corner-extra-small)));
  padding-left: max(16px, var(--mat-form-field-outlined-container-shape, var(--mat-sys-corner-extra-small)) + 4px);
}
[dir=rtl] .mdc-text-field--outlined {
  padding-right: max(16px, var(--mat-form-field-outlined-container-shape, var(--mat-sys-corner-extra-small)) + 4px);
  padding-left: max(16px, var(--mat-form-field-outlined-container-shape, var(--mat-sys-corner-extra-small)));
}

.mdc-floating-label {
  position: absolute;
  left: 0;
  transform-origin: left top;
  line-height: 1.15rem;
  text-align: left;
  text-overflow: ellipsis;
  white-space: nowrap;
  cursor: text;
  overflow: hidden;
  will-change: transform;
}
[dir=rtl] .mdc-floating-label {
  right: 0;
  left: auto;
  transform-origin: right top;
  text-align: right;
}
.mdc-text-field .mdc-floating-label {
  top: 50%;
  transform: translateY(-50%);
  pointer-events: none;
}
.mdc-notched-outline .mdc-floating-label {
  display: inline-block;
  position: relative;
  max-width: 100%;
}
.mdc-text-field--outlined .mdc-floating-label {
  left: 4px;
  right: auto;
}
[dir=rtl] .mdc-text-field--outlined .mdc-floating-label {
  left: auto;
  right: 4px;
}
.mdc-text-field--filled .mdc-floating-label {
  left: 16px;
  right: auto;
}
[dir=rtl] .mdc-text-field--filled .mdc-floating-label {
  left: auto;
  right: 16px;
}
.mdc-text-field--disabled .mdc-floating-label {
  cursor: default;
}
@media (forced-colors: active) {
  .mdc-text-field--disabled .mdc-floating-label {
    z-index: 1;
  }
}
.mdc-text-field--filled.mdc-text-field--no-label .mdc-floating-label {
  display: none;
}
.mdc-text-field--filled:not(.mdc-text-field--disabled) .mdc-floating-label {
  color: var(--mat-form-field-filled-label-text-color, var(--mat-sys-on-surface-variant));
}
.mdc-text-field--filled:not(.mdc-text-field--disabled).mdc-text-field--focused .mdc-floating-label {
  color: var(--mat-form-field-filled-focus-label-text-color, var(--mat-sys-primary));
}
.mdc-text-field--filled:not(.mdc-text-field--disabled):not(.mdc-text-field--focused):hover .mdc-floating-label {
  color: var(--mat-form-field-filled-hover-label-text-color, var(--mat-sys-on-surface-variant));
}
.mdc-text-field--filled.mdc-text-field--disabled .mdc-floating-label {
  color: var(--mat-form-field-filled-disabled-label-text-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
}
.mdc-text-field--filled:not(.mdc-text-field--disabled).mdc-text-field--invalid .mdc-floating-label {
  color: var(--mat-form-field-filled-error-label-text-color, var(--mat-sys-error));
}
.mdc-text-field--filled:not(.mdc-text-field--disabled).mdc-text-field--invalid.mdc-text-field--focused .mdc-floating-label {
  color: var(--mat-form-field-filled-error-focus-label-text-color, var(--mat-sys-error));
}
.mdc-text-field--filled:not(.mdc-text-field--disabled).mdc-text-field--invalid:not(.mdc-text-field--disabled):hover .mdc-floating-label {
  color: var(--mat-form-field-filled-error-hover-label-text-color, var(--mat-sys-on-error-container));
}
.mdc-text-field--filled .mdc-floating-label {
  font-family: var(--mat-form-field-filled-label-text-font, var(--mat-sys-body-large-font));
  font-size: var(--mat-form-field-filled-label-text-size, var(--mat-sys-body-large-size));
  font-weight: var(--mat-form-field-filled-label-text-weight, var(--mat-sys-body-large-weight));
  letter-spacing: var(--mat-form-field-filled-label-text-tracking, var(--mat-sys-body-large-tracking));
}
.mdc-text-field--outlined:not(.mdc-text-field--disabled) .mdc-floating-label {
  color: var(--mat-form-field-outlined-label-text-color, var(--mat-sys-on-surface-variant));
}
.mdc-text-field--outlined:not(.mdc-text-field--disabled).mdc-text-field--focused .mdc-floating-label {
  color: var(--mat-form-field-outlined-focus-label-text-color, var(--mat-sys-primary));
}
.mdc-text-field--outlined:not(.mdc-text-field--disabled):not(.mdc-text-field--focused):hover .mdc-floating-label {
  color: var(--mat-form-field-outlined-hover-label-text-color, var(--mat-sys-on-surface));
}
.mdc-text-field--outlined.mdc-text-field--disabled .mdc-floating-label {
  color: var(--mat-form-field-outlined-disabled-label-text-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
}
.mdc-text-field--outlined:not(.mdc-text-field--disabled).mdc-text-field--invalid .mdc-floating-label {
  color: var(--mat-form-field-outlined-error-label-text-color, var(--mat-sys-error));
}
.mdc-text-field--outlined:not(.mdc-text-field--disabled).mdc-text-field--invalid.mdc-text-field--focused .mdc-floating-label {
  color: var(--mat-form-field-outlined-error-focus-label-text-color, var(--mat-sys-error));
}
.mdc-text-field--outlined:not(.mdc-text-field--disabled).mdc-text-field--invalid:not(.mdc-text-field--disabled):hover .mdc-floating-label {
  color: var(--mat-form-field-outlined-error-hover-label-text-color, var(--mat-sys-on-error-container));
}
.mdc-text-field--outlined .mdc-floating-label {
  font-family: var(--mat-form-field-outlined-label-text-font, var(--mat-sys-body-large-font));
  font-size: var(--mat-form-field-outlined-label-text-size, var(--mat-sys-body-large-size));
  font-weight: var(--mat-form-field-outlined-label-text-weight, var(--mat-sys-body-large-weight));
  letter-spacing: var(--mat-form-field-outlined-label-text-tracking, var(--mat-sys-body-large-tracking));
}

.mdc-floating-label--float-above {
  cursor: auto;
  transform: translateY(-106%) scale(0.75);
}
.mdc-text-field--filled .mdc-floating-label--float-above {
  transform: translateY(-106%) scale(0.75);
}
.mdc-text-field--outlined .mdc-floating-label--float-above {
  transform: translateY(-37.25px) scale(1);
  font-size: 0.75rem;
}
.mdc-notched-outline .mdc-floating-label--float-above {
  text-overflow: clip;
}
.mdc-notched-outline--upgraded .mdc-floating-label--float-above {
  max-width: 133.3333333333%;
}
.mdc-text-field--outlined.mdc-notched-outline--upgraded .mdc-floating-label--float-above, .mdc-text-field--outlined .mdc-notched-outline--upgraded .mdc-floating-label--float-above {
  transform: translateY(-34.75px) scale(0.75);
}
.mdc-text-field--outlined.mdc-notched-outline--upgraded .mdc-floating-label--float-above, .mdc-text-field--outlined .mdc-notched-outline--upgraded .mdc-floating-label--float-above {
  font-size: 1rem;
}

.mdc-floating-label--required:not(.mdc-floating-label--hide-required-marker)::after {
  margin-left: 1px;
  margin-right: 0;
  content: "*";
}
[dir=rtl] .mdc-floating-label--required:not(.mdc-floating-label--hide-required-marker)::after {
  margin-left: 0;
  margin-right: 1px;
}

.mdc-notched-outline {
  display: flex;
  position: absolute;
  top: 0;
  right: 0;
  left: 0;
  box-sizing: border-box;
  width: 100%;
  max-width: 100%;
  height: 100%;
  text-align: left;
  pointer-events: none;
}
[dir=rtl] .mdc-notched-outline {
  text-align: right;
}
.mdc-text-field--outlined .mdc-notched-outline {
  z-index: 1;
}

.mat-mdc-notch-piece {
  box-sizing: border-box;
  height: 100%;
  pointer-events: none;
  border: none;
  border-top: 1px solid;
  border-bottom: 1px solid;
}
.mdc-text-field--focused .mat-mdc-notch-piece {
  border-width: 2px;
}
.mdc-text-field--outlined:not(.mdc-text-field--disabled) .mat-mdc-notch-piece {
  border-color: var(--mat-form-field-outlined-outline-color, var(--mat-sys-outline));
  border-width: var(--mat-form-field-outlined-outline-width, 1px);
}
.mdc-text-field--outlined:not(.mdc-text-field--disabled):not(.mdc-text-field--focused):hover .mat-mdc-notch-piece {
  border-color: var(--mat-form-field-outlined-hover-outline-color, var(--mat-sys-on-surface));
}
.mdc-text-field--outlined:not(.mdc-text-field--disabled).mdc-text-field--focused .mat-mdc-notch-piece {
  border-color: var(--mat-form-field-outlined-focus-outline-color, var(--mat-sys-primary));
}
.mdc-text-field--outlined.mdc-text-field--disabled .mat-mdc-notch-piece {
  border-color: var(--mat-form-field-outlined-disabled-outline-color, color-mix(in srgb, var(--mat-sys-on-surface) 12%, transparent));
}
.mdc-text-field--outlined:not(.mdc-text-field--disabled).mdc-text-field--invalid .mat-mdc-notch-piece {
  border-color: var(--mat-form-field-outlined-error-outline-color, var(--mat-sys-error));
}
.mdc-text-field--outlined:not(.mdc-text-field--disabled).mdc-text-field--invalid:not(.mdc-text-field--focused):hover .mdc-notched-outline .mat-mdc-notch-piece {
  border-color: var(--mat-form-field-outlined-error-hover-outline-color, var(--mat-sys-on-error-container));
}
.mdc-text-field--outlined:not(.mdc-text-field--disabled).mdc-text-field--invalid.mdc-text-field--focused .mat-mdc-notch-piece {
  border-color: var(--mat-form-field-outlined-error-focus-outline-color, var(--mat-sys-error));
}
.mdc-text-field--outlined:not(.mdc-text-field--disabled).mdc-text-field--focused .mdc-notched-outline .mat-mdc-notch-piece {
  border-width: var(--mat-form-field-outlined-focus-outline-width, 2px);
}

.mdc-notched-outline__leading {
  border-left: 1px solid;
  border-right: none;
  border-top-right-radius: 0;
  border-bottom-right-radius: 0;
  border-top-left-radius: var(--mat-form-field-outlined-container-shape, var(--mat-sys-corner-extra-small));
  border-bottom-left-radius: var(--mat-form-field-outlined-container-shape, var(--mat-sys-corner-extra-small));
}
.mdc-text-field--outlined .mdc-notched-outline .mdc-notched-outline__leading {
  width: max(12px, var(--mat-form-field-outlined-container-shape, var(--mat-sys-corner-extra-small)));
}
[dir=rtl] .mdc-notched-outline__leading {
  border-left: none;
  border-right: 1px solid;
  border-bottom-left-radius: 0;
  border-top-left-radius: 0;
  border-top-right-radius: var(--mat-form-field-outlined-container-shape, var(--mat-sys-corner-extra-small));
  border-bottom-right-radius: var(--mat-form-field-outlined-container-shape, var(--mat-sys-corner-extra-small));
}

.mdc-notched-outline__trailing {
  flex-grow: 1;
  border-left: none;
  border-right: 1px solid;
  border-top-left-radius: 0;
  border-bottom-left-radius: 0;
  border-top-right-radius: var(--mat-form-field-outlined-container-shape, var(--mat-sys-corner-extra-small));
  border-bottom-right-radius: var(--mat-form-field-outlined-container-shape, var(--mat-sys-corner-extra-small));
}
[dir=rtl] .mdc-notched-outline__trailing {
  border-left: 1px solid;
  border-right: none;
  border-top-right-radius: 0;
  border-bottom-right-radius: 0;
  border-top-left-radius: var(--mat-form-field-outlined-container-shape, var(--mat-sys-corner-extra-small));
  border-bottom-left-radius: var(--mat-form-field-outlined-container-shape, var(--mat-sys-corner-extra-small));
}

.mdc-notched-outline__notch {
  flex: 0 0 auto;
  width: auto;
}
.mdc-text-field--outlined .mdc-notched-outline .mdc-notched-outline__notch {
  max-width: min(var(--mat-form-field-notch-max-width, 100%), calc(100% - max(12px, var(--mat-form-field-outlined-container-shape, var(--mat-sys-corner-extra-small))) * 2));
}
.mdc-text-field--outlined .mdc-notched-outline--notched .mdc-notched-outline__notch {
  max-width: min(100%, calc(100% - max(12px, var(--mat-form-field-outlined-container-shape, var(--mat-sys-corner-extra-small))) * 2));
}
.mdc-text-field--outlined .mdc-notched-outline--notched .mdc-notched-outline__notch {
  padding-top: 1px;
}
.mdc-text-field--focused.mdc-text-field--outlined .mdc-notched-outline--notched .mdc-notched-outline__notch {
  padding-top: 2px;
}
.mdc-notched-outline--notched .mdc-notched-outline__notch {
  padding-left: 0;
  padding-right: 8px;
  border-top: none;
}
[dir=rtl] .mdc-notched-outline--notched .mdc-notched-outline__notch {
  padding-left: 8px;
  padding-right: 0;
}
.mdc-notched-outline--no-label .mdc-notched-outline__notch {
  display: none;
}

.mdc-line-ripple::before, .mdc-line-ripple::after {
  position: absolute;
  bottom: 0;
  left: 0;
  width: 100%;
  border-bottom-style: solid;
  content: "";
}
.mdc-line-ripple::before {
  z-index: 1;
  border-bottom-width: var(--mat-form-field-filled-active-indicator-height, 1px);
}
.mdc-text-field--filled:not(.mdc-text-field--disabled) .mdc-line-ripple::before {
  border-bottom-color: var(--mat-form-field-filled-active-indicator-color, var(--mat-sys-on-surface-variant));
}
.mdc-text-field--filled:not(.mdc-text-field--disabled):not(.mdc-text-field--focused):hover .mdc-line-ripple::before {
  border-bottom-color: var(--mat-form-field-filled-hover-active-indicator-color, var(--mat-sys-on-surface));
}
.mdc-text-field--filled.mdc-text-field--disabled .mdc-line-ripple::before {
  border-bottom-color: var(--mat-form-field-filled-disabled-active-indicator-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
}
.mdc-text-field--filled:not(.mdc-text-field--disabled).mdc-text-field--invalid .mdc-line-ripple::before {
  border-bottom-color: var(--mat-form-field-filled-error-active-indicator-color, var(--mat-sys-error));
}
.mdc-text-field--filled:not(.mdc-text-field--disabled).mdc-text-field--invalid:not(.mdc-text-field--focused):hover .mdc-line-ripple::before {
  border-bottom-color: var(--mat-form-field-filled-error-hover-active-indicator-color, var(--mat-sys-on-error-container));
}
.mdc-line-ripple::after {
  transform: scaleX(0);
  opacity: 0;
  z-index: 2;
}
.mdc-text-field--filled .mdc-line-ripple::after {
  border-bottom-width: var(--mat-form-field-filled-focus-active-indicator-height, 2px);
}
.mdc-text-field--filled:not(.mdc-text-field--disabled) .mdc-line-ripple::after {
  border-bottom-color: var(--mat-form-field-filled-focus-active-indicator-color, var(--mat-sys-primary));
}
.mdc-text-field--filled.mdc-text-field--invalid:not(.mdc-text-field--disabled) .mdc-line-ripple::after {
  border-bottom-color: var(--mat-form-field-filled-error-focus-active-indicator-color, var(--mat-sys-error));
}

.mdc-line-ripple--active::after {
  transform: scaleX(1);
  opacity: 1;
}

.mdc-line-ripple--deactivating::after {
  opacity: 0;
}

.mdc-text-field--disabled {
  pointer-events: none;
}

.mat-mdc-form-field-textarea-control {
  vertical-align: middle;
  resize: vertical;
  box-sizing: border-box;
  height: auto;
  margin: 0;
  padding: 0;
  border: none;
  overflow: auto;
}

.mat-mdc-form-field-input-control.mat-mdc-form-field-input-control {
  -moz-osx-font-smoothing: grayscale;
  -webkit-font-smoothing: antialiased;
  font: inherit;
  letter-spacing: inherit;
  text-decoration: inherit;
  text-transform: inherit;
  border: none;
}

.mat-mdc-form-field .mat-mdc-floating-label.mdc-floating-label {
  -moz-osx-font-smoothing: grayscale;
  -webkit-font-smoothing: antialiased;
  line-height: normal;
  pointer-events: all;
  will-change: auto;
}

.mat-mdc-form-field:not(.mat-form-field-disabled) .mat-mdc-floating-label.mdc-floating-label {
  cursor: inherit;
}

.mdc-text-field--no-label:not(.mdc-text-field--textarea) .mat-mdc-form-field-input-control.mdc-text-field__input,
.mat-mdc-text-field-wrapper .mat-mdc-form-field-input-control {
  height: auto;
}

.mat-mdc-text-field-wrapper .mat-mdc-form-field-input-control.mdc-text-field__input[type=color] {
  height: 23px;
}

.mat-mdc-text-field-wrapper {
  height: auto;
  flex: auto;
  will-change: auto;
}

.mat-mdc-form-field-has-icon-prefix .mat-mdc-text-field-wrapper {
  padding-left: 0;
  --mat-mdc-form-field-label-offset-x: -16px;
}

.mat-mdc-form-field-has-icon-suffix .mat-mdc-text-field-wrapper {
  padding-right: 0;
}

[dir=rtl] .mat-mdc-text-field-wrapper {
  padding-left: 16px;
  padding-right: 16px;
}
[dir=rtl] .mat-mdc-form-field-has-icon-suffix .mat-mdc-text-field-wrapper {
  padding-left: 0;
}
[dir=rtl] .mat-mdc-form-field-has-icon-prefix .mat-mdc-text-field-wrapper {
  padding-right: 0;
}

.mat-form-field-disabled .mdc-text-field__input::placeholder {
  color: var(--mat-form-field-disabled-input-text-placeholder-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
}
.mat-form-field-disabled .mdc-text-field__input::-moz-placeholder {
  color: var(--mat-form-field-disabled-input-text-placeholder-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
}
.mat-form-field-disabled .mdc-text-field__input::-webkit-input-placeholder {
  color: var(--mat-form-field-disabled-input-text-placeholder-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
}
.mat-form-field-disabled .mdc-text-field__input:-ms-input-placeholder {
  color: var(--mat-form-field-disabled-input-text-placeholder-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
}

.mat-mdc-form-field-label-always-float .mdc-text-field__input::placeholder {
  transition-delay: 40ms;
  transition-duration: 110ms;
  opacity: 1;
}

.mat-mdc-text-field-wrapper .mat-mdc-form-field-infix .mat-mdc-floating-label {
  left: auto;
  right: auto;
}

.mat-mdc-text-field-wrapper.mdc-text-field--outlined .mdc-text-field__input {
  display: inline-block;
}

.mat-mdc-form-field .mat-mdc-text-field-wrapper.mdc-text-field .mdc-notched-outline__notch {
  padding-top: 0;
}

.mat-mdc-form-field.mat-mdc-form-field.mat-mdc-form-field.mat-mdc-form-field.mat-mdc-form-field.mat-mdc-form-field .mdc-notched-outline__notch {
  border-left: 1px solid transparent;
}

[dir=rtl] .mat-mdc-form-field.mat-mdc-form-field.mat-mdc-form-field.mat-mdc-form-field.mat-mdc-form-field.mat-mdc-form-field .mdc-notched-outline__notch {
  border-left: none;
  border-right: 1px solid transparent;
}

.mat-mdc-form-field-infix {
  min-height: var(--mat-form-field-container-height, 56px);
  padding-top: var(--mat-form-field-filled-with-label-container-padding-top, 24px);
  padding-bottom: var(--mat-form-field-filled-with-label-container-padding-bottom, 8px);
}
.mdc-text-field--outlined .mat-mdc-form-field-infix, .mdc-text-field--no-label .mat-mdc-form-field-infix {
  padding-top: var(--mat-form-field-container-vertical-padding, 16px);
  padding-bottom: var(--mat-form-field-container-vertical-padding, 16px);
}

.mat-mdc-text-field-wrapper .mat-mdc-form-field-flex .mat-mdc-floating-label {
  top: calc(var(--mat-form-field-container-height, 56px) / 2);
}

.mdc-text-field--filled .mat-mdc-floating-label {
  display: var(--mat-form-field-filled-label-display, block);
}

.mat-mdc-text-field-wrapper.mdc-text-field--outlined .mdc-notched-outline--upgraded .mdc-floating-label--float-above {
  --mat-mdc-form-field-label-transform: translateY(calc(calc(6.75px + var(--mat-form-field-container-height, 56px) / 2) * -1))
    scale(var(--mat-mdc-form-field-floating-label-scale, 0.75));
  transform: var(--mat-mdc-form-field-label-transform);
}

@keyframes _mat-form-field-subscript-animation {
  from {
    opacity: 0;
    transform: translateY(-5px);
  }
  to {
    opacity: 1;
    transform: translateY(0);
  }
}
.mat-mdc-form-field-subscript-wrapper {
  box-sizing: border-box;
  width: 100%;
  position: relative;
}

.mat-mdc-form-field-hint-wrapper,
.mat-mdc-form-field-error-wrapper {
  position: absolute;
  top: 0;
  left: 0;
  right: 0;
  padding: 0 16px;
  opacity: 1;
  transform: translateY(0);
  animation: _mat-form-field-subscript-animation 0ms cubic-bezier(0.55, 0, 0.55, 0.2);
}

.mat-mdc-form-field-subscript-dynamic-size .mat-mdc-form-field-hint-wrapper,
.mat-mdc-form-field-subscript-dynamic-size .mat-mdc-form-field-error-wrapper {
  position: static;
}

.mat-mdc-form-field-bottom-align::before {
  content: "";
  display: inline-block;
  height: 16px;
}

.mat-mdc-form-field-bottom-align.mat-mdc-form-field-subscript-dynamic-size::before {
  content: unset;
}

.mat-mdc-form-field-hint-end {
  order: 1;
}

.mat-mdc-form-field-hint-wrapper {
  display: flex;
}

.mat-mdc-form-field-hint-spacer {
  flex: 1 0 1em;
}

.mat-mdc-form-field-error {
  display: block;
  color: var(--mat-form-field-error-text-color, var(--mat-sys-error));
}

.mat-mdc-form-field-subscript-wrapper,
.mat-mdc-form-field-bottom-align::before {
  -moz-osx-font-smoothing: grayscale;
  -webkit-font-smoothing: antialiased;
  font-family: var(--mat-form-field-subscript-text-font, var(--mat-sys-body-small-font));
  line-height: var(--mat-form-field-subscript-text-line-height, var(--mat-sys-body-small-line-height));
  font-size: var(--mat-form-field-subscript-text-size, var(--mat-sys-body-small-size));
  letter-spacing: var(--mat-form-field-subscript-text-tracking, var(--mat-sys-body-small-tracking));
  font-weight: var(--mat-form-field-subscript-text-weight, var(--mat-sys-body-small-weight));
}

.mat-mdc-form-field-focus-overlay {
  top: 0;
  left: 0;
  right: 0;
  bottom: 0;
  position: absolute;
  opacity: 0;
  pointer-events: none;
  background-color: var(--mat-form-field-state-layer-color, var(--mat-sys-on-surface));
}
.mat-mdc-text-field-wrapper:hover .mat-mdc-form-field-focus-overlay {
  opacity: var(--mat-form-field-hover-state-layer-opacity, var(--mat-sys-hover-state-layer-opacity));
}
.mat-mdc-form-field.mat-focused .mat-mdc-form-field-focus-overlay {
  opacity: var(--mat-form-field-focus-state-layer-opacity, 0);
}

select.mat-mdc-form-field-input-control {
  -moz-appearance: none;
  -webkit-appearance: none;
  background-color: transparent;
  display: inline-flex;
  box-sizing: border-box;
}
select.mat-mdc-form-field-input-control:not(:disabled) {
  cursor: pointer;
}
select.mat-mdc-form-field-input-control:not(.mat-mdc-native-select-inline) option {
  color: var(--mat-form-field-select-option-text-color, var(--mat-sys-neutral10));
}
select.mat-mdc-form-field-input-control:not(.mat-mdc-native-select-inline) option:disabled {
  color: var(--mat-form-field-select-disabled-option-text-color, color-mix(in srgb, var(--mat-sys-neutral10) 38%, transparent));
}

.mat-mdc-form-field-type-mat-native-select .mat-mdc-form-field-infix::after {
  content: "";
  width: 0;
  height: 0;
  border-left: 5px solid transparent;
  border-right: 5px solid transparent;
  border-top: 5px solid;
  position: absolute;
  right: 0;
  top: 50%;
  margin-top: -2.5px;
  pointer-events: none;
  color: var(--mat-form-field-enabled-select-arrow-color, var(--mat-sys-on-surface-variant));
}
[dir=rtl] .mat-mdc-form-field-type-mat-native-select .mat-mdc-form-field-infix::after {
  right: auto;
  left: 0;
}
.mat-mdc-form-field-type-mat-native-select.mat-focused .mat-mdc-form-field-infix::after {
  color: var(--mat-form-field-focus-select-arrow-color, var(--mat-sys-primary));
}
.mat-mdc-form-field-type-mat-native-select.mat-form-field-disabled .mat-mdc-form-field-infix::after {
  color: var(--mat-form-field-disabled-select-arrow-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
}
.mat-mdc-form-field-type-mat-native-select .mat-mdc-form-field-input-control {
  padding-right: 15px;
}
[dir=rtl] .mat-mdc-form-field-type-mat-native-select .mat-mdc-form-field-input-control {
  padding-right: 0;
  padding-left: 15px;
}

@media (forced-colors: active) {
  .mat-form-field-appearance-fill .mat-mdc-text-field-wrapper {
    outline: solid 1px;
  }
}
@media (forced-colors: active) {
  .mat-form-field-appearance-fill.mat-form-field-disabled .mat-mdc-text-field-wrapper {
    outline-color: GrayText;
  }
}

@media (forced-colors: active) {
  .mat-form-field-appearance-fill.mat-focused .mat-mdc-text-field-wrapper {
    outline: dashed 3px;
  }
}

@media (forced-colors: active) {
  .mat-mdc-form-field.mat-focused .mdc-notched-outline {
    border: dashed 3px;
  }
}

.mat-mdc-form-field-input-control[type=date], .mat-mdc-form-field-input-control[type=datetime], .mat-mdc-form-field-input-control[type=datetime-local], .mat-mdc-form-field-input-control[type=month], .mat-mdc-form-field-input-control[type=week], .mat-mdc-form-field-input-control[type=time] {
  line-height: 1;
}
.mat-mdc-form-field-input-control::-webkit-datetime-edit {
  line-height: 1;
  padding: 0;
  margin-bottom: -2px;
}

.mat-mdc-form-field {
  --mat-mdc-form-field-floating-label-scale: 0.75;
  display: inline-flex;
  flex-direction: column;
  min-width: 0;
  text-align: left;
  -moz-osx-font-smoothing: grayscale;
  -webkit-font-smoothing: antialiased;
  font-family: var(--mat-form-field-container-text-font, var(--mat-sys-body-large-font));
  line-height: var(--mat-form-field-container-text-line-height, var(--mat-sys-body-large-line-height));
  font-size: var(--mat-form-field-container-text-size, var(--mat-sys-body-large-size));
  letter-spacing: var(--mat-form-field-container-text-tracking, var(--mat-sys-body-large-tracking));
  font-weight: var(--mat-form-field-container-text-weight, var(--mat-sys-body-large-weight));
}
.mat-mdc-form-field .mdc-text-field--outlined .mdc-floating-label--float-above {
  font-size: calc(var(--mat-form-field-outlined-label-text-populated-size) * var(--mat-mdc-form-field-floating-label-scale));
}
.mat-mdc-form-field .mdc-text-field--outlined .mdc-notched-outline--upgraded .mdc-floating-label--float-above {
  font-size: var(--mat-form-field-outlined-label-text-populated-size);
}
[dir=rtl] .mat-mdc-form-field {
  text-align: right;
}

.mat-mdc-form-field-flex {
  display: inline-flex;
  align-items: baseline;
  box-sizing: border-box;
  width: 100%;
}

.mat-mdc-text-field-wrapper {
  width: 100%;
  z-index: 0;
}

.mat-mdc-form-field-icon-prefix,
.mat-mdc-form-field-icon-suffix {
  align-self: center;
  line-height: 0;
  pointer-events: auto;
  position: relative;
  z-index: 1;
}
.mat-mdc-form-field-icon-prefix > .mat-icon,
.mat-mdc-form-field-icon-suffix > .mat-icon {
  padding: 0 12px;
  box-sizing: content-box;
}

.mat-mdc-form-field-icon-prefix {
  color: var(--mat-form-field-leading-icon-color, var(--mat-sys-on-surface-variant));
}
.mat-form-field-disabled .mat-mdc-form-field-icon-prefix {
  color: var(--mat-form-field-disabled-leading-icon-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
}

.mat-mdc-form-field-icon-suffix {
  color: var(--mat-form-field-trailing-icon-color, var(--mat-sys-on-surface-variant));
}
.mat-form-field-disabled .mat-mdc-form-field-icon-suffix {
  color: var(--mat-form-field-disabled-trailing-icon-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
}
.mat-form-field-invalid .mat-mdc-form-field-icon-suffix {
  color: var(--mat-form-field-error-trailing-icon-color, var(--mat-sys-error));
}
.mat-form-field-invalid:not(.mat-focused):not(.mat-form-field-disabled) .mat-mdc-text-field-wrapper:hover .mat-mdc-form-field-icon-suffix {
  color: var(--mat-form-field-error-hover-trailing-icon-color, var(--mat-sys-on-error-container));
}
.mat-form-field-invalid.mat-focused .mat-mdc-text-field-wrapper .mat-mdc-form-field-icon-suffix {
  color: var(--mat-form-field-error-focus-trailing-icon-color, var(--mat-sys-error));
}

.mat-mdc-form-field-icon-prefix,
[dir=rtl] .mat-mdc-form-field-icon-suffix {
  padding: 0 4px 0 0;
}

.mat-mdc-form-field-icon-suffix,
[dir=rtl] .mat-mdc-form-field-icon-prefix {
  padding: 0 0 0 4px;
}

.mat-mdc-form-field-subscript-wrapper .mat-icon,
.mat-mdc-form-field label .mat-icon {
  width: 1em;
  height: 1em;
  font-size: inherit;
}

.mat-mdc-form-field-infix {
  flex: auto;
  min-width: 0;
  width: 180px;
  position: relative;
  box-sizing: border-box;
}
.mat-mdc-form-field-infix:has(textarea[cols]) {
  width: auto;
}

.mat-mdc-form-field .mdc-notched-outline__notch {
  margin-left: -1px;
  -webkit-clip-path: inset(-9em -999em -9em 1px);
  clip-path: inset(-9em -999em -9em 1px);
}
[dir=rtl] .mat-mdc-form-field .mdc-notched-outline__notch {
  margin-left: 0;
  margin-right: -1px;
  -webkit-clip-path: inset(-9em 1px -9em -999em);
  clip-path: inset(-9em 1px -9em -999em);
}

.mat-mdc-form-field.mat-form-field-animations-enabled .mdc-floating-label {
  transition: transform 150ms cubic-bezier(0.4, 0, 0.2, 1), color 150ms cubic-bezier(0.4, 0, 0.2, 1);
}
.mat-mdc-form-field.mat-form-field-animations-enabled .mdc-text-field__input {
  transition: opacity 150ms cubic-bezier(0.4, 0, 0.2, 1);
}
.mat-mdc-form-field.mat-form-field-animations-enabled .mdc-text-field__input::placeholder {
  transition: opacity 67ms cubic-bezier(0.4, 0, 0.2, 1);
}
.mat-mdc-form-field.mat-form-field-animations-enabled .mdc-text-field__input::-moz-placeholder {
  transition: opacity 67ms cubic-bezier(0.4, 0, 0.2, 1);
}
.mat-mdc-form-field.mat-form-field-animations-enabled .mdc-text-field__input::-webkit-input-placeholder {
  transition: opacity 67ms cubic-bezier(0.4, 0, 0.2, 1);
}
.mat-mdc-form-field.mat-form-field-animations-enabled .mdc-text-field__input:-ms-input-placeholder {
  transition: opacity 67ms cubic-bezier(0.4, 0, 0.2, 1);
}
.mat-mdc-form-field.mat-form-field-animations-enabled.mdc-text-field--no-label .mdc-text-field__input::placeholder, .mat-mdc-form-field.mat-form-field-animations-enabled.mdc-text-field--focused .mdc-text-field__input::placeholder {
  transition-delay: 40ms;
  transition-duration: 110ms;
}
.mat-mdc-form-field.mat-form-field-animations-enabled.mdc-text-field--no-label .mdc-text-field__input::-moz-placeholder, .mat-mdc-form-field.mat-form-field-animations-enabled.mdc-text-field--focused .mdc-text-field__input::-moz-placeholder {
  transition-delay: 40ms;
  transition-duration: 110ms;
}
.mat-mdc-form-field.mat-form-field-animations-enabled.mdc-text-field--no-label .mdc-text-field__input::-webkit-input-placeholder, .mat-mdc-form-field.mat-form-field-animations-enabled.mdc-text-field--focused .mdc-text-field__input::-webkit-input-placeholder {
  transition-delay: 40ms;
  transition-duration: 110ms;
}
.mat-mdc-form-field.mat-form-field-animations-enabled.mdc-text-field--no-label .mdc-text-field__input:-ms-input-placeholder, .mat-mdc-form-field.mat-form-field-animations-enabled.mdc-text-field--focused .mdc-text-field__input:-ms-input-placeholder {
  transition-delay: 40ms;
  transition-duration: 110ms;
}
.mat-mdc-form-field.mat-form-field-animations-enabled .mdc-text-field--filled:not(.mdc-ripple-upgraded):focus .mdc-text-field__ripple::before {
  transition-duration: 75ms;
}
.mat-mdc-form-field.mat-form-field-animations-enabled .mdc-line-ripple::after {
  transition: transform 180ms cubic-bezier(0.4, 0, 0.2, 1), opacity 180ms cubic-bezier(0.4, 0, 0.2, 1);
}
.mat-mdc-form-field.mat-form-field-animations-enabled .mat-mdc-form-field-hint-wrapper,
.mat-mdc-form-field.mat-form-field-animations-enabled .mat-mdc-form-field-error-wrapper {
  animation-duration: 300ms;
}

.mdc-notched-outline .mdc-floating-label {
  max-width: calc(100% + 1px);
}

.mdc-notched-outline--upgraded .mdc-floating-label--float-above {
  max-width: calc(133.3333333333% + 1px);
}
`],encapsulation:2,changeDetection:0})}return t})();var qj=["text"],Yj=[[["mat-icon"]],"*"],Zj=["mat-icon","*"];function Qj(t,n){if(t&1&&O(0,"mat-pseudo-checkbox",1),t&2){let e=S();D("disabled",e.disabled)("state",e.selected?"checked":"unchecked")}}function Xj(t,n){if(t&1&&O(0,"mat-pseudo-checkbox",3),t&2){let e=S();D("disabled",e.disabled)}}function Kj(t,n){if(t&1&&(f(0,"span",4),_(1),h()),t&2){let e=S();p(),we("(",e.group.label,")")}}var uy=new y("MAT_OPTION_PARENT_COMPONENT"),my=new y("MatOptgroup");var dy=class{source;isUserInput;constructor(n,e=!1){this.source=n,this.isUserInput=e}},sh=(()=>{class t{_element=d(L);_changeDetectorRef=d(ve);_parent=d(uy,{optional:!0});group=d(my,{optional:!0});_signalDisableRipple=!1;_selected=!1;_active=!1;_mostRecentViewValue="";get multiple(){return this._parent&&this._parent.multiple}get selected(){return this._selected}value;id=d(Ie).getId("mat-option-");get disabled(){return this.group&&this.group.disabled||this._disabled()}set disabled(e){this._disabled.set(e)}_disabled=M(!1);get disableRipple(){return this._signalDisableRipple?this._parent.disableRipple():!!this._parent?.disableRipple}get hideSingleSelectionIndicator(){return!!(this._parent&&this._parent.hideSingleSelectionIndicator)}onSelectionChange=new U;_text;_stateChanges=new I;constructor(){let e=d(qe);e.load(An),e.load(Qi),this._signalDisableRipple=!!this._parent&&$i(this._parent.disableRipple)}get active(){return this._active}get viewValue(){return(this._text?.nativeElement.textContent||"").trim()}select(e=!0){this._selected||(this._selected=!0,this._changeDetectorRef.markForCheck(),e&&this._emitSelectionChangeEvent())}deselect(e=!0){this._selected&&(this._selected=!1,this._changeDetectorRef.markForCheck(),e&&this._emitSelectionChangeEvent())}focus(e,i){let r=this._getHostElement();typeof r.focus=="function"&&r.focus(i)}setActiveStyles(){this._active||(this._active=!0,this._changeDetectorRef.markForCheck())}setInactiveStyles(){this._active&&(this._active=!1,this._changeDetectorRef.markForCheck())}getLabel(){return this.viewValue}_handleKeydown(e){(e.keyCode===13||e.keyCode===32)&&!Ct(e)&&(this._selectViaInteraction(),e.preventDefault())}_selectViaInteraction(){this.disabled||(this._selected=this.multiple?!this._selected:!0,this._changeDetectorRef.markForCheck(),this._emitSelectionChangeEvent(!0))}_getTabIndex(){return this.disabled?"-1":"0"}_getHostElement(){return this._element.nativeElement}ngAfterViewChecked(){if(this._selected){let e=this.viewValue;e!==this._mostRecentViewValue&&(this._mostRecentViewValue&&this._stateChanges.next(),this._mostRecentViewValue=e)}}ngOnDestroy(){this._stateChanges.complete()}_emitSelectionChangeEvent(e=!1){this.onSelectionChange.emit(new dy(this,e))}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=E({type:t,selectors:[["mat-option"]],viewQuery:function(i,r){if(i&1&&Ve(qj,7),i&2){let o;$(o=G())&&(r._text=o.first)}},hostAttrs:["role","option",1,"mat-mdc-option","mdc-list-item"],hostVars:11,hostBindings:function(i,r){i&1&&T("click",function(){return r._selectViaInteraction()})("keydown",function(s){return r._handleKeydown(s)}),i&2&&(mt("id",r.id),q("aria-selected",r.selected)("aria-disabled",r.disabled.toString()),z("mdc-list-item--selected",r.selected)("mat-mdc-option-multiple",r.multiple)("mat-mdc-option-active",r.active)("mdc-list-item--disabled",r.disabled))},inputs:{value:"value",id:"id",disabled:[2,"disabled","disabled",B]},outputs:{onSelectionChange:"onSelectionChange"},exportAs:["matOption"],ngContentSelectors:Zj,decls:8,vars:5,consts:[["text",""],["aria-hidden","true",1,"mat-mdc-option-pseudo-checkbox",3,"disabled","state"],[1,"mdc-list-item__primary-text"],["state","checked","aria-hidden","true","appearance","minimal",1,"mat-mdc-option-pseudo-checkbox",3,"disabled"],[1,"cdk-visually-hidden"],["aria-hidden","true","mat-ripple","",1,"mat-mdc-option-ripple","mat-focus-indicator",3,"matRippleTrigger","matRippleDisabled"]],template:function(i,r){i&1&&(Ce(Yj),N(0,Qj,1,2,"mat-pseudo-checkbox",1),re(1),f(2,"span",2,0),re(4,1),h(),N(5,Xj,1,1,"mat-pseudo-checkbox",3),N(6,Kj,2,1,"span",4),O(7,"div",5)),i&2&&(P(r.multiple?0:-1),p(5),P(!r.multiple&&r.selected&&!r.hideSingleSelectionIndicator?5:-1),p(),P(r.group&&r.group._inert?6:-1),p(),D("matRippleTrigger",r._getHostElement())("matRippleDisabled",r.disabled||r.disableRipple))},dependencies:[RI,Xi],styles:[`.mat-mdc-option {
  -webkit-user-select: none;
  user-select: none;
  -moz-osx-font-smoothing: grayscale;
  -webkit-font-smoothing: antialiased;
  display: flex;
  position: relative;
  align-items: center;
  justify-content: flex-start;
  overflow: hidden;
  min-height: 48px;
  padding: 0 16px;
  cursor: pointer;
  -webkit-tap-highlight-color: transparent;
  color: var(--mat-option-label-text-color, var(--mat-sys-on-surface));
  font-family: var(--mat-option-label-text-font, var(--mat-sys-label-large-font));
  line-height: var(--mat-option-label-text-line-height, var(--mat-sys-label-large-line-height));
  font-size: var(--mat-option-label-text-size, var(--mat-sys-body-large-size));
  letter-spacing: var(--mat-option-label-text-tracking, var(--mat-sys-label-large-tracking));
  font-weight: var(--mat-option-label-text-weight, var(--mat-sys-body-large-weight));
}
.mat-mdc-option:hover:not(.mdc-list-item--disabled) {
  background-color: var(--mat-option-hover-state-layer-color, color-mix(in srgb, var(--mat-sys-on-surface) calc(var(--mat-sys-hover-state-layer-opacity) * 100%), transparent));
}
.mat-mdc-option:focus.mdc-list-item, .mat-mdc-option.mat-mdc-option-active.mdc-list-item {
  background-color: var(--mat-option-focus-state-layer-color, color-mix(in srgb, var(--mat-sys-on-surface) calc(var(--mat-sys-focus-state-layer-opacity) * 100%), transparent));
  outline: 0;
}
.mat-mdc-option.mdc-list-item--selected:not(.mdc-list-item--disabled):not(.mat-mdc-option-active, .mat-mdc-option-multiple, :focus, :hover) {
  background-color: var(--mat-option-selected-state-layer-color, var(--mat-sys-secondary-container));
}
.mat-mdc-option.mdc-list-item--selected:not(.mdc-list-item--disabled):not(.mat-mdc-option-active, .mat-mdc-option-multiple, :focus, :hover) .mdc-list-item__primary-text {
  color: var(--mat-option-selected-state-label-text-color, var(--mat-sys-on-secondary-container));
}
.mat-mdc-option .mat-pseudo-checkbox {
  --mat-pseudo-checkbox-minimal-selected-checkmark-color: var(--mat-option-selected-state-label-text-color, var(--mat-sys-on-secondary-container));
}
.mat-mdc-option.mdc-list-item {
  align-items: center;
  background: transparent;
}
.mat-mdc-option.mdc-list-item--disabled {
  cursor: default;
  pointer-events: none;
}
.mat-mdc-option.mdc-list-item--disabled .mat-mdc-option-pseudo-checkbox, .mat-mdc-option.mdc-list-item--disabled .mdc-list-item__primary-text, .mat-mdc-option.mdc-list-item--disabled > mat-icon {
  opacity: 0.38;
}
.mat-mdc-optgroup .mat-mdc-option:not(.mat-mdc-option-multiple) {
  padding-left: 32px;
}
[dir=rtl] .mat-mdc-optgroup .mat-mdc-option:not(.mat-mdc-option-multiple) {
  padding-left: 16px;
  padding-right: 32px;
}
.mat-mdc-option .mat-icon,
.mat-mdc-option .mat-pseudo-checkbox-full {
  margin-right: 16px;
  flex-shrink: 0;
}
[dir=rtl] .mat-mdc-option .mat-icon,
[dir=rtl] .mat-mdc-option .mat-pseudo-checkbox-full {
  margin-right: 0;
  margin-left: 16px;
}
.mat-mdc-option .mat-pseudo-checkbox-minimal {
  margin-left: 16px;
  flex-shrink: 0;
}
[dir=rtl] .mat-mdc-option .mat-pseudo-checkbox-minimal {
  margin-right: 16px;
  margin-left: 0;
}
.mat-mdc-option .mat-mdc-option-ripple {
  top: 0;
  left: 0;
  right: 0;
  bottom: 0;
  position: absolute;
  pointer-events: none;
}
.mat-mdc-option .mdc-list-item__primary-text {
  white-space: normal;
  font-size: inherit;
  font-weight: inherit;
  letter-spacing: inherit;
  line-height: inherit;
  font-family: inherit;
  text-decoration: inherit;
  text-transform: inherit;
  margin-right: auto;
}
[dir=rtl] .mat-mdc-option .mdc-list-item__primary-text {
  margin-right: 0;
  margin-left: auto;
}
@media (forced-colors: active) {
  .mat-mdc-option.mdc-list-item--selected:not(:has(.mat-mdc-option-pseudo-checkbox))::after {
    content: "";
    position: absolute;
    top: 50%;
    right: 16px;
    transform: translateY(-50%);
    width: 10px;
    height: 0;
    border-bottom: solid 10px;
    border-radius: 10px;
  }
  [dir=rtl] .mat-mdc-option.mdc-list-item--selected:not(:has(.mat-mdc-option-pseudo-checkbox))::after {
    right: auto;
    left: 16px;
  }
}

.mat-mdc-option-multiple {
  --mat-list-list-item-selected-container-color: var(--mat-list-list-item-container-color, transparent);
}

.mat-mdc-option-active .mat-focus-indicator::before {
  content: "";
}
`],encapsulation:2,changeDetection:0})}return t})();function QI(t,n,e){if(e.length){let i=n.toArray(),r=e.toArray(),o=0;for(let s=0;s<t+1;s++)i[s].group&&i[s].group===r[o]&&o++;return o}return 0}function XI(t,n,e,i){return t<e?t:t+n>e+i?Math.max(0,t-i+n):e}var go=(()=>{class t{isErrorState(e,i){return!!(e&&e.invalid&&(e.touched||i&&i.submitted))}static \u0275fac=function(i){return new(i||t)};static \u0275prov=C({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();var Wa=class{_defaultMatcher;ngControl;_parentFormGroup;_parentForm;_stateChanges;errorState=!1;matcher;constructor(n,e,i,r,o){this._defaultMatcher=n,this.ngControl=e,this._parentFormGroup=i,this._parentForm=r,this._stateChanges=o}updateErrorState(){let n=this.errorState,e=this._parentFormGroup||this._parentForm,i=this.matcher||this._defaultMatcher,r=this.ngControl?this.ngControl.control:null,o=i?.isErrorState(r,e)??!1;o!==n&&(this.errorState=o,this._stateChanges.next())}};var Jj=["trigger"],eB=["panel"],tB=[[["mat-select-trigger"]],"*"],nB=["mat-select-trigger","*"];function iB(t,n){if(t&1&&(f(0,"span",4),_(1),h()),t&2){let e=S();p(),V(e.placeholder)}}function rB(t,n){t&1&&re(0)}function oB(t,n){if(t&1&&(f(0,"span",11),_(1),h()),t&2){let e=S(2);p(),V(e.triggerValue)}}function sB(t,n){if(t&1&&(f(0,"span",5),N(1,rB,1,0)(2,oB,2,1,"span",11),h()),t&2){let e=S();p(),P(e.customTrigger?1:2)}}function aB(t,n){if(t&1){let e=Mt();f(0,"div",12,1),T("keydown",function(r){Te(e);let o=S();return Ae(o._handleKeydown(r))}),re(2,1),h()}if(t&2){let e=S();Tt(e.panelClass),z("mat-select-panel-animations-enabled",!e._animationsDisabled)("mat-primary",(e._parentFormField==null?null:e._parentFormField.color)==="primary")("mat-accent",(e._parentFormField==null?null:e._parentFormField.color)==="accent")("mat-warn",(e._parentFormField==null?null:e._parentFormField.color)==="warn")("mat-undefined",!(e._parentFormField!=null&&e._parentFormField.color)),q("id",e.id+"-panel")("aria-multiselectable",e.multiple)("aria-label",e.ariaLabel||null)("aria-labelledby",e._getPanelAriaLabelledby())}}var cB=new y("mat-select-scroll-strategy",{providedIn:"root",factory:()=>{let t=d(X);return()=>lo(t)}}),lB=new y("MAT_SELECT_CONFIG"),dB=new y("MatSelectTrigger"),fy=class{source;value;constructor(n,e){this.source=n,this.value=e}},KI=(()=>{class t{_viewportRuler=d(ao);_changeDetectorRef=d(ve);_elementRef=d(L);_dir=d(Xt,{optional:!0});_idGenerator=d(Ie);_renderer=d(xe);_parentFormField=d(md,{optional:!0});ngControl=d(Cn,{self:!0,optional:!0});_liveAnnouncer=d(Pl);_defaultOptions=d(lB,{optional:!0});_animationsDisabled=ke();_popoverLocation;_initialized=new I;_cleanupDetach;options;optionGroups;customTrigger;_positions=[{originX:"start",originY:"bottom",overlayX:"start",overlayY:"top"},{originX:"end",originY:"bottom",overlayX:"end",overlayY:"top"},{originX:"start",originY:"top",overlayX:"start",overlayY:"bottom",panelClass:"mat-mdc-select-panel-above"},{originX:"end",originY:"top",overlayX:"end",overlayY:"bottom",panelClass:"mat-mdc-select-panel-above"}];_scrollOptionIntoView(e){let i=this.options.toArray()[e];if(i){let r=this.panel.nativeElement,o=QI(e,this.options,this.optionGroups),s=i._getHostElement();e===0&&o===1?r.scrollTop=0:r.scrollTop=XI(s.offsetTop,s.offsetHeight,r.scrollTop,r.offsetHeight)}}_positioningSettled(){this._scrollOptionIntoView(this._keyManager.activeItemIndex||0)}_getChangeEvent(e){return new fy(this,e)}_scrollStrategyFactory=d(cB);_panelOpen=!1;_compareWith=(e,i)=>e===i;_uid=this._idGenerator.getId("mat-select-");_triggerAriaLabelledBy=null;_previousControl;_destroy=new I;_errorStateTracker;stateChanges=new I;disableAutomaticLabeling=!0;userAriaDescribedBy;_selectionModel;_keyManager;_preferredOverlayOrigin;_overlayWidth;_onChange=()=>{};_onTouched=()=>{};_valueId=this._idGenerator.getId("mat-select-value-");_scrollStrategy;_overlayPanelClass=this._defaultOptions?.overlayPanelClass||"";get focused(){return this._focused||this._panelOpen}_focused=!1;controlType="mat-select";trigger;panel;_overlayDir;panelClass;disabled=!1;get disableRipple(){return this._disableRipple()}set disableRipple(e){this._disableRipple.set(e)}_disableRipple=M(!1);tabIndex=0;get hideSingleSelectionIndicator(){return this._hideSingleSelectionIndicator}set hideSingleSelectionIndicator(e){this._hideSingleSelectionIndicator=e,this._syncParentProperties()}_hideSingleSelectionIndicator=this._defaultOptions?.hideSingleSelectionIndicator??!1;get placeholder(){return this._placeholder}set placeholder(e){this._placeholder=e,this.stateChanges.next()}_placeholder;get required(){return this._required??this.ngControl?.control?.hasValidator(Jt.required)??!1}set required(e){this._required=e,this.stateChanges.next()}_required;get multiple(){return this._multiple}set multiple(e){this._selectionModel,this._multiple=e}_multiple=!1;disableOptionCentering=this._defaultOptions?.disableOptionCentering??!1;get compareWith(){return this._compareWith}set compareWith(e){this._compareWith=e,this._selectionModel&&this._initializeSelection()}get value(){return this._value}set value(e){this._assignValue(e)&&this._onChange(e)}_value;ariaLabel="";ariaLabelledby;get errorStateMatcher(){return this._errorStateTracker.matcher}set errorStateMatcher(e){this._errorStateTracker.matcher=e}typeaheadDebounceInterval;sortComparator;get id(){return this._id}set id(e){this._id=e||this._uid,this.stateChanges.next()}_id;get errorState(){return this._errorStateTracker.errorState}set errorState(e){this._errorStateTracker.errorState=e}panelWidth=this._defaultOptions&&typeof this._defaultOptions.panelWidth<"u"?this._defaultOptions.panelWidth:"auto";canSelectNullableOptions=this._defaultOptions?.canSelectNullableOptions??!1;optionSelectionChanges=ci(()=>{let e=this.options;return e?e.changes.pipe(et(e),gt(()=>mn(...e.map(i=>i.onSelectionChange)))):this._initialized.pipe(gt(()=>this.optionSelectionChanges))});openedChange=new U;_openedStream=this.openedChange.pipe(De(e=>e),Q(()=>{}));_closedStream=this.openedChange.pipe(De(e=>!e),Q(()=>{}));selectionChange=new U;valueChange=new U;constructor(){let e=d(go),i=d(ys,{optional:!0}),r=d(Dr,{optional:!0}),o=d(new Kn("tabindex"),{optional:!0}),s=d(Yl,{optional:!0});this.ngControl&&(this.ngControl.valueAccessor=this),this._defaultOptions?.typeaheadDebounceInterval!=null&&(this.typeaheadDebounceInterval=this._defaultOptions.typeaheadDebounceInterval),this._errorStateTracker=new Wa(e,this.ngControl,r,i,this.stateChanges),this._scrollStrategy=this._scrollStrategyFactory(),this.tabIndex=o==null?0:parseInt(o)||0,this._popoverLocation=s?.usePopover===!1?null:"inline",this.id=this.id}ngOnInit(){this._selectionModel=new mo(this.multiple),this.stateChanges.next(),this._viewportRuler.change().pipe(me(this._destroy)).subscribe(()=>{this.panelOpen&&(this._overlayWidth=this._getOverlayWidth(this._preferredOverlayOrigin),this._changeDetectorRef.detectChanges())})}ngAfterContentInit(){this._initialized.next(),this._initialized.complete(),this._initKeyManager(),this._selectionModel.changed.pipe(me(this._destroy)).subscribe(e=>{e.added.forEach(i=>i.select()),e.removed.forEach(i=>i.deselect())}),this.options.changes.pipe(et(null),me(this._destroy)).subscribe(()=>{this._resetOptions(),this._initializeSelection()})}ngDoCheck(){let e=this._getTriggerAriaLabelledby(),i=this.ngControl;if(e!==this._triggerAriaLabelledBy){let r=this._elementRef.nativeElement;this._triggerAriaLabelledBy=e,e?r.setAttribute("aria-labelledby",e):r.removeAttribute("aria-labelledby")}i&&(this._previousControl!==i.control&&(this._previousControl!==void 0&&i.disabled!==null&&i.disabled!==this.disabled&&(this.disabled=i.disabled),this._previousControl=i.control),this.updateErrorState())}ngOnChanges(e){(e.disabled||e.userAriaDescribedBy)&&this.stateChanges.next(),e.typeaheadDebounceInterval&&this._keyManager&&this._keyManager.withTypeAhead(this.typeaheadDebounceInterval),e.panelClass&&this.panelClass instanceof Set&&(this.panelClass=Array.from(this.panelClass))}ngOnDestroy(){this._cleanupDetach?.(),this._keyManager?.destroy(),this._destroy.next(),this._destroy.complete(),this.stateChanges.complete(),this._clearFromModal()}toggle(){this.panelOpen?this.close():this.open()}open(){this._canOpen()&&(this._parentFormField&&(this._preferredOverlayOrigin=this._parentFormField.getConnectedOverlayOrigin()),this._cleanupDetach?.(),this._overlayWidth=this._getOverlayWidth(this._preferredOverlayOrigin),this._applyModalPanelOwnership(),this._panelOpen=!0,this._overlayDir.positionChange.pipe(ut(1)).subscribe(()=>{this._changeDetectorRef.detectChanges(),this._positioningSettled()}),this._overlayDir.attachOverlay(),this._keyManager.withHorizontalOrientation(null),this._highlightCorrectOption(),this._changeDetectorRef.markForCheck(),this.stateChanges.next(),Promise.resolve().then(()=>this.openedChange.emit(!0)))}_trackedModal=null;_applyModalPanelOwnership(){let e=this._elementRef.nativeElement.closest('body > .cdk-overlay-container [aria-modal="true"]');if(!e)return;let i=`${this.id}-panel`;this._trackedModal&&Ef(this._trackedModal,"aria-owns",i),wb(e,"aria-owns",i),this._trackedModal=e}_clearFromModal(){if(!this._trackedModal)return;let e=`${this.id}-panel`;Ef(this._trackedModal,"aria-owns",e),this._trackedModal=null}close(){this._panelOpen&&(this._panelOpen=!1,this._exitAndDetach(),this._keyManager.withHorizontalOrientation(this._isRtl()?"rtl":"ltr"),this._changeDetectorRef.markForCheck(),this._onTouched(),this.stateChanges.next(),Promise.resolve().then(()=>this.openedChange.emit(!1)))}_exitAndDetach(){if(this._animationsDisabled||!this.panel){this._detachOverlay();return}this._cleanupDetach?.(),this._cleanupDetach=()=>{i(),clearTimeout(r),this._cleanupDetach=void 0};let e=this.panel.nativeElement,i=this._renderer.listen(e,"animationend",o=>{o.animationName==="_mat-select-exit"&&(this._cleanupDetach?.(),this._detachOverlay())}),r=setTimeout(()=>{this._cleanupDetach?.(),this._detachOverlay()},200);e.classList.add("mat-select-panel-exit")}_detachOverlay(){this._overlayDir.detachOverlay(),this._changeDetectorRef.markForCheck()}writeValue(e){this._assignValue(e)}registerOnChange(e){this._onChange=e}registerOnTouched(e){this._onTouched=e}setDisabledState(e){this.disabled=e,this._changeDetectorRef.markForCheck(),this.stateChanges.next()}get panelOpen(){return this._panelOpen}get selected(){return this.multiple?this._selectionModel?.selected||[]:this._selectionModel?.selected[0]}get triggerValue(){if(this.empty)return"";if(this._multiple){let e=this._selectionModel.selected.map(i=>i.viewValue);return this._isRtl()&&e.reverse(),e.join(", ")}return this._selectionModel.selected[0].viewValue}updateErrorState(){this._errorStateTracker.updateErrorState()}_isRtl(){return this._dir?this._dir.value==="rtl":!1}_handleKeydown(e){this.disabled||(this.panelOpen?this._handleOpenKeydown(e):this._handleClosedKeydown(e))}_handleClosedKeydown(e){let i=e.keyCode,r=i===40||i===38||i===37||i===39,o=i===13||i===32,s=this._keyManager;if(!s.isTyping()&&o&&!Ct(e)||(this.multiple||e.altKey)&&r)e.preventDefault(),this.open();else if(!this.multiple){let a=this.selected;s.onKeydown(e);let c=this.selected;c&&a!==c&&this._liveAnnouncer.announce(c.viewValue,1e4)}}_handleOpenKeydown(e){let i=this._keyManager,r=e.keyCode,o=r===40||r===38,s=i.isTyping();if(o&&e.altKey)e.preventDefault(),this.close();else if(!s&&(r===13||r===32)&&i.activeItem&&!Ct(e))e.preventDefault(),i.activeItem._selectViaInteraction();else if(!s&&this._multiple&&r===65&&e.ctrlKey){e.preventDefault();let a=this.options.some(c=>!c.disabled&&!c.selected);this.options.forEach(c=>{c.disabled||(a?c.select():c.deselect())})}else{let a=i.activeItemIndex;i.onKeydown(e),this._multiple&&o&&e.shiftKey&&i.activeItem&&i.activeItemIndex!==a&&i.activeItem._selectViaInteraction()}}_handleOverlayKeydown(e){e.keyCode===27&&!Ct(e)&&(e.preventDefault(),this.close())}_onFocus(){this.disabled||(this._focused=!0,this.stateChanges.next())}_onBlur(){this._focused=!1,this._keyManager?.cancelTypeahead(),!this.disabled&&!this.panelOpen&&(this._onTouched(),this._changeDetectorRef.markForCheck(),this.stateChanges.next())}get empty(){return!this._selectionModel||this._selectionModel.isEmpty()}_initializeSelection(){Promise.resolve().then(()=>{this.ngControl&&(this._value=this.ngControl.value),this._setSelectionByValue(this._value),this.stateChanges.next()})}_setSelectionByValue(e){if(this.options.forEach(i=>i.setInactiveStyles()),this._selectionModel.clear(),this.multiple&&e)Array.isArray(e),e.forEach(i=>this._selectOptionByValue(i)),this._sortValues();else{let i=this._selectOptionByValue(e);i?this._keyManager.updateActiveItem(i):this.panelOpen||this._keyManager.updateActiveItem(-1)}this._changeDetectorRef.markForCheck()}_selectOptionByValue(e){let i=this.options.find(r=>{if(this._selectionModel.isSelected(r))return!1;try{return(r.value!=null||this.canSelectNullableOptions)&&this._compareWith(r.value,e)}catch{return!1}});return i&&this._selectionModel.select(i),i}_assignValue(e){return e!==this._value||this._multiple&&Array.isArray(e)?(this.options&&this._setSelectionByValue(e),this._value=e,!0):!1}_skipPredicate=e=>this.panelOpen?!1:e.disabled;_getOverlayWidth(e){return this.panelWidth==="auto"?(e instanceof Oa?e.elementRef:e||this._elementRef).nativeElement.getBoundingClientRect().width:this.panelWidth===null?"":this.panelWidth}_syncParentProperties(){if(this.options)for(let e of this.options)e._changeDetectorRef.markForCheck()}_initKeyManager(){this._keyManager=new Vl(this.options).withTypeAhead(this.typeaheadDebounceInterval).withVerticalOrientation().withHorizontalOrientation(this._isRtl()?"rtl":"ltr").withHomeAndEnd().withPageUpDown().withAllowedModifierKeys(["shiftKey"]).skipPredicate(this._skipPredicate),this._keyManager.tabOut.subscribe(()=>{this.panelOpen&&(!this.multiple&&this._keyManager.activeItem&&this._keyManager.activeItem._selectViaInteraction(),this.focus(),this.close())}),this._keyManager.change.subscribe(()=>{this._panelOpen&&this.panel?this._scrollOptionIntoView(this._keyManager.activeItemIndex||0):!this._panelOpen&&!this.multiple&&this._keyManager.activeItem&&this._keyManager.activeItem._selectViaInteraction()})}_resetOptions(){let e=mn(this.options.changes,this._destroy);this.optionSelectionChanges.pipe(me(e)).subscribe(i=>{this._onSelect(i.source,i.isUserInput),i.isUserInput&&!this.multiple&&this._panelOpen&&(this.close(),this.focus())}),mn(...this.options.map(i=>i._stateChanges)).pipe(me(e)).subscribe(()=>{this._changeDetectorRef.detectChanges(),this.stateChanges.next()})}_onSelect(e,i){let r=this._selectionModel.isSelected(e);!this.canSelectNullableOptions&&e.value==null&&!this._multiple?(e.deselect(),this._selectionModel.clear(),this.value!=null&&this._propagateChanges(e.value)):(r!==e.selected&&(e.selected?this._selectionModel.select(e):this._selectionModel.deselect(e)),i&&this._keyManager.setActiveItem(e),this.multiple&&(this._sortValues(),i&&this.focus())),r!==this._selectionModel.isSelected(e)&&this._propagateChanges(),this.stateChanges.next()}_sortValues(){if(this.multiple){let e=this.options.toArray();this._selectionModel.sort((i,r)=>this.sortComparator?this.sortComparator(i,r,e):e.indexOf(i)-e.indexOf(r)),this.stateChanges.next()}}_propagateChanges(e){let i;this.multiple?i=this.selected.map(r=>r.value):i=this.selected?this.selected.value:e,this._value=i,this.valueChange.emit(i),this._onChange(i),this.selectionChange.emit(this._getChangeEvent(i)),this._changeDetectorRef.markForCheck()}_highlightCorrectOption(){if(this._keyManager)if(this.empty){let e=-1;for(let i=0;i<this.options.length;i++)if(!this.options.get(i).disabled){e=i;break}this._keyManager.setActiveItem(e)}else this._keyManager.setActiveItem(this._selectionModel.selected[0])}_canOpen(){return!this._panelOpen&&!this.disabled&&this.options?.length>0&&!!this._overlayDir}focus(e){this._elementRef.nativeElement.focus(e)}_getPanelAriaLabelledby(){if(this.ariaLabel)return null;let e=this._parentFormField?.getLabelId()||null,i=e?e+" ":"";return this.ariaLabelledby?i+this.ariaLabelledby:e}_getAriaActiveDescendant(){return this.panelOpen&&this._keyManager&&this._keyManager.activeItem?this._keyManager.activeItem.id:null}_getTriggerAriaLabelledby(){if(this.ariaLabel)return null;let e=this._parentFormField?.getLabelId()||"";return this.ariaLabelledby&&(e+=" "+this.ariaLabelledby),e||(e=this._valueId),e}get describedByIds(){return this._elementRef.nativeElement.getAttribute("aria-describedby")?.split(" ")||[]}setDescribedByIds(e){let i=this._elementRef.nativeElement;e.length?i.setAttribute("aria-describedby",e.join(" ")):i.removeAttribute("aria-describedby")}onContainerClick(e){let i=Kt(e);i&&(i.tagName==="MAT-OPTION"||i.classList.contains("cdk-overlay-backdrop")||i.closest(".mat-mdc-select-panel"))||(this.focus(),this.open())}get shouldLabelFloat(){return this.panelOpen||!this.empty||this.focused&&!!this.placeholder}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=E({type:t,selectors:[["mat-select"]],contentQueries:function(i,r,o){if(i&1&&yt(o,dB,5)(o,sh,5)(o,my,5),i&2){let s;$(s=G())&&(r.customTrigger=s.first),$(s=G())&&(r.options=s),$(s=G())&&(r.optionGroups=s)}},viewQuery:function(i,r){if(i&1&&Ve(Jj,5)(eB,5)(Bf,5),i&2){let o;$(o=G())&&(r.trigger=o.first),$(o=G())&&(r.panel=o.first),$(o=G())&&(r._overlayDir=o.first)}},hostAttrs:["role","combobox","aria-haspopup","listbox",1,"mat-mdc-select"],hostVars:21,hostBindings:function(i,r){i&1&&T("keydown",function(s){return r._handleKeydown(s)})("focus",function(){return r._onFocus()})("blur",function(){return r._onBlur()}),i&2&&(q("id",r.id)("tabindex",r.disabled?-1:r.tabIndex)("aria-controls",r.panelOpen?r.id+"-panel":null)("aria-expanded",r.panelOpen)("aria-label",r.ariaLabel||null)("aria-required",r.required.toString())("aria-disabled",r.disabled.toString())("aria-invalid",r.errorState)("aria-activedescendant",r._getAriaActiveDescendant()),z("mat-mdc-select-disabled",r.disabled)("mat-mdc-select-invalid",r.errorState)("mat-mdc-select-required",r.required)("mat-mdc-select-empty",r.empty)("mat-mdc-select-multiple",r.multiple)("mat-select-open",r.panelOpen))},inputs:{userAriaDescribedBy:[0,"aria-describedby","userAriaDescribedBy"],panelClass:"panelClass",disabled:[2,"disabled","disabled",B],disableRipple:[2,"disableRipple","disableRipple",B],tabIndex:[2,"tabIndex","tabIndex",e=>e==null?0:lt(e)],hideSingleSelectionIndicator:[2,"hideSingleSelectionIndicator","hideSingleSelectionIndicator",B],placeholder:"placeholder",required:[2,"required","required",B],multiple:[2,"multiple","multiple",B],disableOptionCentering:[2,"disableOptionCentering","disableOptionCentering",B],compareWith:"compareWith",value:"value",ariaLabel:[0,"aria-label","ariaLabel"],ariaLabelledby:[0,"aria-labelledby","ariaLabelledby"],errorStateMatcher:"errorStateMatcher",typeaheadDebounceInterval:[2,"typeaheadDebounceInterval","typeaheadDebounceInterval",lt],sortComparator:"sortComparator",id:"id",panelWidth:"panelWidth",canSelectNullableOptions:[2,"canSelectNullableOptions","canSelectNullableOptions",B]},outputs:{openedChange:"openedChange",_openedStream:"opened",_closedStream:"closed",selectionChange:"selectionChange",valueChange:"valueChange"},exportAs:["matSelect"],features:[Ee([{provide:ud,useExisting:t},{provide:uy,useExisting:t}]),ze],ngContentSelectors:nB,decls:11,vars:10,consts:[["fallbackOverlayOrigin","cdkOverlayOrigin","trigger",""],["panel",""],["cdk-overlay-origin","",1,"mat-mdc-select-trigger",3,"click"],[1,"mat-mdc-select-value"],[1,"mat-mdc-select-placeholder","mat-mdc-select-min-line"],[1,"mat-mdc-select-value-text"],[1,"mat-mdc-select-arrow-wrapper"],[1,"mat-mdc-select-arrow"],["viewBox","0 0 24 24","width","24px","height","24px","focusable","false","aria-hidden","true"],["d","M7 10l5 5 5-5z"],["cdk-connected-overlay","","cdkConnectedOverlayHasBackdrop","","cdkConnectedOverlayBackdropClass","cdk-overlay-transparent-backdrop",3,"detach","backdropClick","overlayKeydown","cdkConnectedOverlayDisableClose","cdkConnectedOverlayPanelClass","cdkConnectedOverlayScrollStrategy","cdkConnectedOverlayOrigin","cdkConnectedOverlayPositions","cdkConnectedOverlayWidth","cdkConnectedOverlayFlexibleDimensions","cdkConnectedOverlayUsePopover"],[1,"mat-mdc-select-min-line"],["role","listbox","tabindex","-1",1,"mat-mdc-select-panel","mdc-menu-surface","mdc-menu-surface--open",3,"keydown"]],template:function(i,r){if(i&1&&(Ce(tB),f(0,"div",2,0),T("click",function(){return r.open()}),f(3,"div",3),N(4,iB,2,1,"span",4)(5,sB,3,1,"span",5),h(),f(6,"div",6)(7,"div",7),Gt(),f(8,"svg",8),O(9,"path",9),h()()()(),$e(10,aB,3,16,"ng-template",10),T("detach",function(){return r.close()})("backdropClick",function(){return r.close()})("overlayKeydown",function(s){return r._handleOverlayKeydown(s)})),i&2){let o=Re(1);p(3),q("id",r._valueId),p(),P(r.empty?4:5),p(6),D("cdkConnectedOverlayDisableClose",!0)("cdkConnectedOverlayPanelClass",r._overlayPanelClass)("cdkConnectedOverlayScrollStrategy",r._scrollStrategy)("cdkConnectedOverlayOrigin",r._preferredOverlayOrigin||o)("cdkConnectedOverlayPositions",r._positions)("cdkConnectedOverlayWidth",r._overlayWidth)("cdkConnectedOverlayFlexibleDimensions",!0)("cdkConnectedOverlayUsePopover",r._popoverLocation)}},dependencies:[Oa,Bf],styles:[`@keyframes _mat-select-enter {
  from {
    opacity: 0;
    transform: scaleY(0.8);
  }
  to {
    opacity: 1;
    transform: none;
  }
}
@keyframes _mat-select-exit {
  from {
    opacity: 1;
  }
  to {
    opacity: 0;
  }
}
.mat-mdc-select {
  display: inline-block;
  width: 100%;
  outline: none;
  -moz-osx-font-smoothing: grayscale;
  -webkit-font-smoothing: antialiased;
  color: var(--mat-select-enabled-trigger-text-color, var(--mat-sys-on-surface));
  font-family: var(--mat-select-trigger-text-font, var(--mat-sys-body-large-font));
  line-height: var(--mat-select-trigger-text-line-height, var(--mat-sys-body-large-line-height));
  font-size: var(--mat-select-trigger-text-size, var(--mat-sys-body-large-size));
  font-weight: var(--mat-select-trigger-text-weight, var(--mat-sys-body-large-weight));
  letter-spacing: var(--mat-select-trigger-text-tracking, var(--mat-sys-body-large-tracking));
}

div.mat-mdc-select-panel {
  box-shadow: var(--mat-select-container-elevation-shadow, 0px 3px 1px -2px rgba(0, 0, 0, 0.2), 0px 2px 2px 0px rgba(0, 0, 0, 0.14), 0px 1px 5px 0px rgba(0, 0, 0, 0.12));
}

.mat-mdc-select-disabled {
  color: var(--mat-select-disabled-trigger-text-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
}
.mat-mdc-select-disabled .mat-mdc-select-placeholder {
  color: var(--mat-select-disabled-trigger-text-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
}

.mat-mdc-select-trigger {
  display: inline-flex;
  align-items: center;
  cursor: pointer;
  position: relative;
  box-sizing: border-box;
  width: 100%;
}
.mat-mdc-select-disabled .mat-mdc-select-trigger {
  -webkit-user-select: none;
  user-select: none;
  cursor: default;
}

.mat-mdc-select-value {
  width: 100%;
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
}

.mat-mdc-select-value-text {
  white-space: nowrap;
  overflow: hidden;
  text-overflow: ellipsis;
}

.mat-mdc-select-arrow-wrapper {
  height: 24px;
  flex-shrink: 0;
  display: inline-flex;
  align-items: center;
}
.mat-form-field-appearance-fill .mdc-text-field--no-label .mat-mdc-select-arrow-wrapper {
  transform: none;
}

.mat-mdc-form-field .mat-mdc-select.mat-mdc-select-invalid .mat-mdc-select-arrow,
.mat-form-field-invalid:not(.mat-form-field-disabled) .mat-mdc-form-field-infix::after {
  color: var(--mat-select-invalid-arrow-color, var(--mat-sys-error));
}

.mat-mdc-select-arrow {
  width: 10px;
  height: 5px;
  position: relative;
  color: var(--mat-select-enabled-arrow-color, var(--mat-sys-on-surface-variant));
}
.mat-mdc-form-field.mat-focused .mat-mdc-select-arrow {
  color: var(--mat-select-focused-arrow-color, var(--mat-sys-primary));
}
.mat-mdc-form-field .mat-mdc-select.mat-mdc-select-disabled .mat-mdc-select-arrow {
  color: var(--mat-select-disabled-arrow-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
}
.mat-select-open .mat-mdc-select-arrow {
  transform: rotate(180deg);
}
.mat-form-field-animations-enabled .mat-mdc-select-arrow {
  transition: transform 80ms linear;
}
.mat-mdc-select-arrow svg {
  fill: currentColor;
  position: absolute;
  top: 50%;
  left: 50%;
  transform: translate(-50%, -50%);
}
@media (forced-colors: active) {
  .mat-mdc-select-arrow svg {
    fill: CanvasText;
  }
  .mat-mdc-select-disabled .mat-mdc-select-arrow svg {
    fill: GrayText;
  }
}

div.mat-mdc-select-panel {
  width: 100%;
  max-height: 275px;
  outline: 0;
  overflow: auto;
  padding: 8px 0;
  box-sizing: border-box;
  transform-origin: top center;
  border-radius: 0 0 4px 4px;
  position: relative;
  background-color: var(--mat-select-panel-background-color, var(--mat-sys-surface-container));
}
.mat-mdc-select-panel-above div.mat-mdc-select-panel {
  border-radius: 4px 4px 0 0;
  transform-origin: bottom center;
}
@media (forced-colors: active) {
  div.mat-mdc-select-panel {
    outline: solid 1px;
  }
}

.mat-select-panel-animations-enabled {
  animation: _mat-select-enter 120ms cubic-bezier(0, 0, 0.2, 1);
}
.mat-select-panel-animations-enabled.mat-select-panel-exit {
  animation: _mat-select-exit 100ms linear;
}

.mat-mdc-select-placeholder {
  transition: color 400ms 133.3333333333ms cubic-bezier(0.25, 0.8, 0.25, 1);
  color: var(--mat-select-placeholder-text-color, var(--mat-sys-on-surface-variant));
}
.mat-mdc-form-field:not(.mat-form-field-animations-enabled) .mat-mdc-select-placeholder, ._mat-animation-noopable .mat-mdc-select-placeholder {
  transition: none;
}
.mat-form-field-hide-placeholder .mat-mdc-select-placeholder {
  color: transparent;
  -webkit-text-fill-color: transparent;
  transition: none;
  display: block;
}

.mat-mdc-form-field-type-mat-select:not(.mat-form-field-disabled) .mat-mdc-text-field-wrapper {
  cursor: pointer;
}
.mat-mdc-form-field-type-mat-select.mat-form-field-appearance-fill .mat-mdc-floating-label {
  max-width: calc(100% - 18px);
}
.mat-mdc-form-field-type-mat-select.mat-form-field-appearance-fill .mdc-floating-label--float-above {
  max-width: calc(100% / 0.75 - 24px);
}
.mat-mdc-form-field-type-mat-select.mat-form-field-appearance-outline .mdc-notched-outline__notch {
  max-width: calc(100% - 60px);
}
.mat-mdc-form-field-type-mat-select.mat-form-field-appearance-outline .mdc-text-field--label-floating .mdc-notched-outline__notch {
  max-width: calc(100% - 24px);
}

.mat-mdc-select-min-line:empty::before {
  content: " ";
  white-space: pre;
  width: 1px;
  display: inline-block;
  visibility: hidden;
}

.mat-form-field-appearance-fill .mat-mdc-select-arrow-wrapper {
  transform: var(--mat-select-arrow-transform, translateY(-8px));
}
`],encapsulation:2,changeDetection:0})}return t})();var uB=["tooltip"],mB=20;var fB=new y("mat-tooltip-scroll-strategy",{providedIn:"root",factory:()=>{let t=d(X);return()=>lo(t,{scrollThrottle:mB})}}),hB=new y("mat-tooltip-default-options",{providedIn:"root",factory:()=>({showDelay:0,hideDelay:0,touchendHideDelay:1500})});var JI="tooltip-panel",pB={passive:!0},gB=8,_B=8,vB=24,bB=200,ek=(()=>{class t{_elementRef=d(L);_ngZone=d(H);_platform=d(ye);_ariaDescriber=d(Sf);_focusMonitor=d(Tn);_dir=d(Xt);_injector=d(X);_viewContainerRef=d(Vt);_mediaMatcher=d(Sa);_document=d(K);_renderer=d(xe);_animationsDisabled=ke();_defaultOptions=d(hB,{optional:!0});_overlayRef=null;_tooltipInstance=null;_overlayPanelClass;_portal;_position="below";_positionAtOrigin=!1;_disabled=!1;_tooltipClass;_viewInitialized=!1;_pointerExitEventsInitialized=!1;_tooltipComponent=yB;_viewportMargin=8;_currentPosition;_cssClassPrefix="mat-mdc";_ariaDescriptionPending=!1;_dirSubscribed=!1;get position(){return this._position}set position(e){e!==this._position&&(this._position=e,this._overlayRef&&(this._updatePosition(this._overlayRef),this._tooltipInstance?.show(0),this._overlayRef.updatePosition()))}get positionAtOrigin(){return this._positionAtOrigin}set positionAtOrigin(e){this._positionAtOrigin=Rt(e),this._detach(),this._overlayRef=null}get disabled(){return this._disabled}set disabled(e){let i=Rt(e);this._disabled!==i&&(this._disabled=i,i?this.hide(0):this._setupPointerEnterEventsIfNeeded(),this._syncAriaDescription(this.message))}get showDelay(){return this._showDelay}set showDelay(e){this._showDelay=Hn(e)}_showDelay;get hideDelay(){return this._hideDelay}set hideDelay(e){this._hideDelay=Hn(e),this._tooltipInstance&&(this._tooltipInstance._mouseLeaveHideDelay=this._hideDelay)}_hideDelay;touchGestures="auto";get message(){return this._message}set message(e){let i=this._message;this._message=e!=null?String(e).trim():"",!this._message&&this._isTooltipVisible()?this.hide(0):(this._setupPointerEnterEventsIfNeeded(),this._updateTooltipMessage()),this._syncAriaDescription(i)}_message="";get tooltipClass(){return this._tooltipClass}set tooltipClass(e){this._tooltipClass=e,this._tooltipInstance&&this._setTooltipClass(this._tooltipClass)}_eventCleanups=[];_touchstartTimeout=null;_destroyed=new I;_isDestroyed=!1;constructor(){let e=this._defaultOptions;e&&(this._showDelay=e.showDelay,this._hideDelay=e.hideDelay,e.position&&(this.position=e.position),e.positionAtOrigin&&(this.positionAtOrigin=e.positionAtOrigin),e.touchGestures&&(this.touchGestures=e.touchGestures),e.tooltipClass&&(this.tooltipClass=e.tooltipClass)),this._viewportMargin=gB}ngAfterViewInit(){this._viewInitialized=!0,this._setupPointerEnterEventsIfNeeded(),this._focusMonitor.monitor(this._elementRef).pipe(me(this._destroyed)).subscribe(e=>{e?e==="keyboard"&&this._ngZone.run(()=>this.show()):this._ngZone.run(()=>this.hide(0))})}ngOnDestroy(){let e=this._elementRef.nativeElement;this._touchstartTimeout&&clearTimeout(this._touchstartTimeout),this._overlayRef&&(this._overlayRef.dispose(),this._tooltipInstance=null),this._eventCleanups.forEach(i=>i()),this._eventCleanups.length=0,this._destroyed.next(),this._destroyed.complete(),this._isDestroyed=!0,this._ariaDescriber.removeDescription(e,this.message,"tooltip"),this._focusMonitor.stopMonitoring(e)}show(e=this.showDelay,i){if(this.disabled||!this.message||this._isTooltipVisible()){this._tooltipInstance?._cancelPendingAnimations();return}let r=this._createOverlay(i);this._detach(),this._portal=this._portal||new Ci(this._tooltipComponent,this._viewContainerRef);let o=this._tooltipInstance=r.attach(this._portal).instance;o._triggerElement=this._elementRef.nativeElement,o._mouseLeaveHideDelay=this._hideDelay,o.afterHidden().pipe(me(this._destroyed)).subscribe(()=>this._detach()),this._setTooltipClass(this._tooltipClass),this._updateTooltipMessage(),o.show(e)}hide(e=this.hideDelay){let i=this._tooltipInstance;i&&(i.isVisible()?i.hide(e):(i._cancelPendingAnimations(),this._detach()))}toggle(e){this._isTooltipVisible()?this.hide():this.show(void 0,e)}_isTooltipVisible(){return!!this._tooltipInstance&&this._tooltipInstance.isVisible()}_createOverlay(e){if(this._overlayRef){let s=this._overlayRef.getConfig().positionStrategy;if((!this.positionAtOrigin||!e)&&s._origin instanceof L)return this._overlayRef;this._detach()}let i=this._injector.get(Ta).getAncestorScrollContainers(this._elementRef),r=`${this._cssClassPrefix}-${JI}`,o=Na(this._injector,this.positionAtOrigin?e||this._elementRef:this._elementRef).withTransformOriginOn(`.${this._cssClassPrefix}-tooltip`).withFlexibleDimensions(!1).withViewportMargin(this._viewportMargin).withScrollableContainers(i).withPopoverLocation("global");return o.positionChanges.pipe(me(this._destroyed)).subscribe(s=>{this._updateCurrentPositionClass(s.connectionPair),this._tooltipInstance&&s.scrollableViewProperties.isOverlayClipped&&this._tooltipInstance.isVisible()&&this._ngZone.run(()=>this.hide(0))}),this._overlayRef=Ji(this._injector,{direction:this._dir,positionStrategy:o,panelClass:this._overlayPanelClass?[...this._overlayPanelClass,r]:r,scrollStrategy:this._injector.get(fB)(),disableAnimations:this._animationsDisabled,eventPredicate:this._overlayEventPredicate}),this._updatePosition(this._overlayRef),this._overlayRef.detachments().pipe(me(this._destroyed)).subscribe(()=>this._detach()),this._overlayRef.outsidePointerEvents().pipe(me(this._destroyed)).subscribe(()=>this._tooltipInstance?._handleBodyInteraction()),this._overlayRef.keydownEvents().pipe(me(this._destroyed)).subscribe(s=>{s.preventDefault(),s.stopPropagation(),this._ngZone.run(()=>this.hide(0))}),this._defaultOptions?.disableTooltipInteractivity&&this._overlayRef.addPanelClass(`${this._cssClassPrefix}-tooltip-panel-non-interactive`),this._dirSubscribed||(this._dirSubscribed=!0,this._dir.change.pipe(me(this._destroyed)).subscribe(()=>{this._overlayRef&&this._updatePosition(this._overlayRef)})),this._overlayRef}_detach(){this._overlayRef&&this._overlayRef.hasAttached()&&this._overlayRef.detach(),this._tooltipInstance=null}_updatePosition(e){let i=e.getConfig().positionStrategy,r=this._getOrigin(),o=this._getOverlayPosition();i.withPositions([this._addOffset(b(b({},r.main),o.main)),this._addOffset(b(b({},r.fallback),o.fallback))])}_addOffset(e){let i=_B,r=!this._dir||this._dir.value=="ltr";return e.originY==="top"?e.offsetY=-i:e.originY==="bottom"?e.offsetY=i:e.originX==="start"?e.offsetX=r?-i:i:e.originX==="end"&&(e.offsetX=r?i:-i),e}_getOrigin(){let e=!this._dir||this._dir.value=="ltr",i=this.position,r;i=="above"||i=="below"?r={originX:"center",originY:i=="above"?"top":"bottom"}:i=="before"||i=="left"&&e||i=="right"&&!e?r={originX:"start",originY:"center"}:(i=="after"||i=="right"&&e||i=="left"&&!e)&&(r={originX:"end",originY:"center"});let{x:o,y:s}=this._invertPosition(r.originX,r.originY);return{main:r,fallback:{originX:o,originY:s}}}_getOverlayPosition(){let e=!this._dir||this._dir.value=="ltr",i=this.position,r;i=="above"?r={overlayX:"center",overlayY:"bottom"}:i=="below"?r={overlayX:"center",overlayY:"top"}:i=="before"||i=="left"&&e||i=="right"&&!e?r={overlayX:"end",overlayY:"center"}:(i=="after"||i=="right"&&e||i=="left"&&!e)&&(r={overlayX:"start",overlayY:"center"});let{x:o,y:s}=this._invertPosition(r.overlayX,r.overlayY);return{main:r,fallback:{overlayX:o,overlayY:s}}}_updateTooltipMessage(){this._tooltipInstance&&(this._tooltipInstance.message=this.message,this._tooltipInstance._markForCheck(),bt(()=>{this._tooltipInstance&&this._overlayRef.updatePosition()},{injector:this._injector}))}_setTooltipClass(e){this._tooltipInstance&&(this._tooltipInstance.tooltipClass=e instanceof Set?Array.from(e):e,this._tooltipInstance._markForCheck())}_invertPosition(e,i){return this.position==="above"||this.position==="below"?i==="top"?i="bottom":i==="bottom"&&(i="top"):e==="end"?e="start":e==="start"&&(e="end"),{x:e,y:i}}_updateCurrentPositionClass(e){let{overlayY:i,originX:r,originY:o}=e,s;if(i==="center"?this._dir&&this._dir.value==="rtl"?s=r==="end"?"left":"right":s=r==="start"?"left":"right":s=i==="bottom"&&o==="top"?"above":"below",s!==this._currentPosition){let a=this._overlayRef;if(a){let c=`${this._cssClassPrefix}-${JI}-`;a.removePanelClass(c+this._currentPosition),a.addPanelClass(c+s)}this._currentPosition=s}}_setupPointerEnterEventsIfNeeded(){this._disabled||!this.message||!this._viewInitialized||this._eventCleanups.length||(this._isTouchPlatform()?this.touchGestures!=="off"&&(this._disableNativeGesturesIfNecessary(),this._addListener("touchstart",e=>{let i=e.targetTouches?.[0],r=i?{x:i.clientX,y:i.clientY}:void 0;this._setupPointerExitEventsIfNeeded(),this._touchstartTimeout&&clearTimeout(this._touchstartTimeout);let o=500;this._touchstartTimeout=setTimeout(()=>{this._touchstartTimeout=null,this.show(void 0,r)},this._defaultOptions?.touchLongPressShowDelay??o)})):this._addListener("mouseenter",e=>{this._setupPointerExitEventsIfNeeded();let i;e.x!==void 0&&e.y!==void 0&&(i=e),this.show(void 0,i)}))}_setupPointerExitEventsIfNeeded(){if(!this._pointerExitEventsInitialized){if(this._pointerExitEventsInitialized=!0,!this._isTouchPlatform())this._addListener("mouseleave",e=>{let i=e.relatedTarget;(!i||!this._overlayRef?.overlayElement.contains(i))&&this.hide()}),this._addListener("wheel",e=>{if(this._isTooltipVisible()){let i=this._document.elementFromPoint(e.clientX,e.clientY),r=this._elementRef.nativeElement;i!==r&&!r.contains(i)&&this.hide()}});else if(this.touchGestures!=="off"){this._disableNativeGesturesIfNecessary();let e=()=>{this._touchstartTimeout&&clearTimeout(this._touchstartTimeout),this.hide(this._defaultOptions?.touchendHideDelay)};this._addListener("touchend",e),this._addListener("touchcancel",e)}}}_addListener(e,i){this._eventCleanups.push(this._renderer.listen(this._elementRef.nativeElement,e,i,pB))}_isTouchPlatform(){let e=this._defaultOptions?.detectHoverCapability;return typeof e=="function"?!e():this._platform.IOS||this._platform.ANDROID?!0:this._platform.isBrowser?!!e&&this._mediaMatcher.matchMedia("(any-hover: none)").matches:!1}_disableNativeGesturesIfNecessary(){let e=this.touchGestures;if(e!=="off"){let i=this._elementRef.nativeElement,r=i.style;(e==="on"||i.nodeName!=="INPUT"&&i.nodeName!=="TEXTAREA")&&(r.userSelect=r.msUserSelect=r.webkitUserSelect=r.MozUserSelect="none"),(e==="on"||!i.draggable)&&(r.webkitUserDrag="none"),r.touchAction="none",r.webkitTapHighlightColor="transparent"}}_syncAriaDescription(e){this._ariaDescriptionPending||(this._ariaDescriptionPending=!0,this._ariaDescriber.removeDescription(this._elementRef.nativeElement,e,"tooltip"),this._isDestroyed||bt({write:()=>{this._ariaDescriptionPending=!1,this.message&&!this.disabled&&this._ariaDescriber.describe(this._elementRef.nativeElement,this.message,"tooltip")}},{injector:this._injector}))}_overlayEventPredicate=e=>e.type==="keydown"?this._isTooltipVisible()&&e.keyCode===27&&!Ct(e):!0;static \u0275fac=function(i){return new(i||t)};static \u0275dir=A({type:t,selectors:[["","matTooltip",""]],hostAttrs:[1,"mat-mdc-tooltip-trigger"],hostVars:2,hostBindings:function(i,r){i&2&&z("mat-mdc-tooltip-disabled",r.disabled)},inputs:{position:[0,"matTooltipPosition","position"],positionAtOrigin:[0,"matTooltipPositionAtOrigin","positionAtOrigin"],disabled:[0,"matTooltipDisabled","disabled"],showDelay:[0,"matTooltipShowDelay","showDelay"],hideDelay:[0,"matTooltipHideDelay","hideDelay"],touchGestures:[0,"matTooltipTouchGestures","touchGestures"],message:[0,"matTooltip","message"],tooltipClass:[0,"matTooltipClass","tooltipClass"]},exportAs:["matTooltip"]})}return t})(),yB=(()=>{class t{_changeDetectorRef=d(ve);_elementRef=d(L);_isMultiline=!1;message;tooltipClass;_showTimeoutId;_hideTimeoutId;_triggerElement;_mouseLeaveHideDelay;_animationsDisabled=ke();_tooltip;_closeOnInteraction=!1;_isVisible=!1;_onHide=new I;_showAnimation="mat-mdc-tooltip-show";_hideAnimation="mat-mdc-tooltip-hide";constructor(){}show(e){this._hideTimeoutId!=null&&clearTimeout(this._hideTimeoutId),this._showTimeoutId=setTimeout(()=>{this._toggleVisibility(!0),this._showTimeoutId=void 0},e)}hide(e){this._showTimeoutId!=null&&clearTimeout(this._showTimeoutId),this._hideTimeoutId=setTimeout(()=>{this._toggleVisibility(!1),this._hideTimeoutId=void 0},e)}afterHidden(){return this._onHide}isVisible(){return this._isVisible}ngOnDestroy(){this._cancelPendingAnimations(),this._onHide.complete(),this._triggerElement=null}_handleBodyInteraction(){this._closeOnInteraction&&this.hide(0)}_markForCheck(){this._changeDetectorRef.markForCheck()}_handleMouseLeave({relatedTarget:e}){(!e||!this._triggerElement.contains(e))&&(this.isVisible()?this.hide(this._mouseLeaveHideDelay):this._finalizeAnimation(!1))}_onShow(){this._isMultiline=this._isTooltipMultiline(),this._markForCheck()}_isTooltipMultiline(){let e=this._elementRef.nativeElement.getBoundingClientRect();return e.height>vB&&e.width>=bB}_handleAnimationEnd({animationName:e}){(e===this._showAnimation||e===this._hideAnimation)&&this._finalizeAnimation(e===this._showAnimation)}_cancelPendingAnimations(){this._showTimeoutId!=null&&clearTimeout(this._showTimeoutId),this._hideTimeoutId!=null&&clearTimeout(this._hideTimeoutId),this._showTimeoutId=this._hideTimeoutId=void 0}_finalizeAnimation(e){e?this._closeOnInteraction=!0:this.isVisible()||this._onHide.next()}_toggleVisibility(e){let i=this._tooltip.nativeElement,r=this._showAnimation,o=this._hideAnimation;if(i.classList.remove(e?o:r),i.classList.add(e?r:o),this._isVisible!==e&&(this._isVisible=e,this._changeDetectorRef.markForCheck()),e&&!this._animationsDisabled&&typeof getComputedStyle=="function"){let s=getComputedStyle(i);(s.getPropertyValue("animation-duration")==="0s"||s.getPropertyValue("animation-name")==="none")&&(this._animationsDisabled=!0)}e&&this._onShow(),this._animationsDisabled&&(i.classList.add("_mat-animation-noopable"),this._finalizeAnimation(e))}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=E({type:t,selectors:[["mat-tooltip-component"]],viewQuery:function(i,r){if(i&1&&Ve(uB,7),i&2){let o;$(o=G())&&(r._tooltip=o.first)}},hostAttrs:["aria-hidden","true"],hostBindings:function(i,r){i&1&&T("mouseleave",function(s){return r._handleMouseLeave(s)})},decls:4,vars:5,consts:[["tooltip",""],[1,"mdc-tooltip","mat-mdc-tooltip",3,"animationend"],[1,"mat-mdc-tooltip-surface","mdc-tooltip__surface"]],template:function(i,r){i&1&&(Fe(0,"div",1,0),na("animationend",function(s){return r._handleAnimationEnd(s)}),Fe(2,"div",2),_(3),Ge()()),i&2&&(Tt(r.tooltipClass),z("mdc-tooltip--multiline",r._isMultiline),p(3),V(r.message))},styles:[`.mat-mdc-tooltip {
  position: relative;
  transform: scale(0);
  display: inline-flex;
}
.mat-mdc-tooltip::before {
  content: "";
  top: 0;
  right: 0;
  bottom: 0;
  left: 0;
  z-index: -1;
  position: absolute;
}
.mat-mdc-tooltip-panel-below .mat-mdc-tooltip::before {
  top: -8px;
}
.mat-mdc-tooltip-panel-above .mat-mdc-tooltip::before {
  bottom: -8px;
}
.mat-mdc-tooltip-panel-right .mat-mdc-tooltip::before {
  left: -8px;
}
.mat-mdc-tooltip-panel-left .mat-mdc-tooltip::before {
  right: -8px;
}
.mat-mdc-tooltip._mat-animation-noopable {
  animation: none;
  transform: scale(1);
}

.mat-mdc-tooltip-surface {
  word-break: normal;
  overflow-wrap: anywhere;
  padding: 4px 8px;
  min-width: 40px;
  max-width: 200px;
  min-height: 24px;
  max-height: 40vh;
  box-sizing: border-box;
  overflow: hidden;
  text-align: center;
  will-change: transform, opacity;
  background-color: var(--mat-tooltip-container-color, var(--mat-sys-inverse-surface));
  color: var(--mat-tooltip-supporting-text-color, var(--mat-sys-inverse-on-surface));
  border-radius: var(--mat-tooltip-container-shape, var(--mat-sys-corner-extra-small));
  font-family: var(--mat-tooltip-supporting-text-font, var(--mat-sys-body-small-font));
  font-size: var(--mat-tooltip-supporting-text-size, var(--mat-sys-body-small-size));
  font-weight: var(--mat-tooltip-supporting-text-weight, var(--mat-sys-body-small-weight));
  line-height: var(--mat-tooltip-supporting-text-line-height, var(--mat-sys-body-small-line-height));
  letter-spacing: var(--mat-tooltip-supporting-text-tracking, var(--mat-sys-body-small-tracking));
}
.mat-mdc-tooltip-surface::before {
  position: absolute;
  box-sizing: border-box;
  width: 100%;
  height: 100%;
  top: 0;
  left: 0;
  border: 1px solid transparent;
  border-radius: inherit;
  content: "";
  pointer-events: none;
}
.mdc-tooltip--multiline .mat-mdc-tooltip-surface {
  text-align: left;
}
[dir=rtl] .mdc-tooltip--multiline .mat-mdc-tooltip-surface {
  text-align: right;
}

.mat-mdc-tooltip-panel {
  line-height: normal;
}
.mat-mdc-tooltip-panel.mat-mdc-tooltip-panel-non-interactive {
  pointer-events: none;
}

@keyframes mat-mdc-tooltip-show {
  0% {
    opacity: 0;
    transform: scale(0.8);
  }
  100% {
    opacity: 1;
    transform: scale(1);
  }
}
@keyframes mat-mdc-tooltip-hide {
  0% {
    opacity: 1;
    transform: scale(1);
  }
  100% {
    opacity: 0;
    transform: scale(0.8);
  }
}
.mat-mdc-tooltip-show {
  animation: mat-mdc-tooltip-show 150ms cubic-bezier(0, 0, 0.2, 1) forwards;
}

.mat-mdc-tooltip-hide {
  animation: mat-mdc-tooltip-hide 75ms cubic-bezier(0.4, 0, 1, 1) forwards;
}
`],encapsulation:2,changeDetection:0})}return t})();function xB(t,n){if(t&1&&(f(0,"mat-option",17),_(1),h()),t&2){let e=n.$implicit;D("value",e),p(),we(" ",e," ")}}function CB(t,n){if(t&1){let e=Mt();f(0,"mat-form-field",14)(1,"mat-select",16,0),T("selectionChange",function(r){Te(e);let o=S(2);return Ae(o._changePageSize(r.value))}),tt(3,xB,2,2,"mat-option",17,ns),h(),f(5,"div",18),T("click",function(){Te(e);let r=Re(2);return Ae(r.open())}),h()()}if(t&2){let e=S(2);D("appearance",e._formFieldAppearance)("color",e.color),p(),D("value",e.pageSize)("disabled",e.disabled),vm("aria-labelledby",e._pageSizeLabelId),D("panelClass",e.selectConfig.panelClass||"")("disableOptionCentering",e.selectConfig.disableOptionCentering),p(2),nt(e._displayedPageSizeOptions)}}function wB(t,n){if(t&1&&(f(0,"div",15),_(1),h()),t&2){let e=S(2);p(),V(e.pageSize)}}function DB(t,n){if(t&1&&(f(0,"div",3)(1,"div",13),_(2),h(),N(3,CB,6,7,"mat-form-field",14),N(4,wB,2,1,"div",15),h()),t&2){let e=S();p(),q("id",e._pageSizeLabelId),p(),we(" ",e._intl.itemsPerPageLabel," "),p(),P(e._displayedPageSizeOptions.length>1?3:-1),p(),P(e._displayedPageSizeOptions.length<=1?4:-1)}}function EB(t,n){if(t&1){let e=Mt();f(0,"button",19),T("click",function(){Te(e);let r=S();return Ae(r._buttonClicked(0,r._previousButtonsDisabled()))}),Gt(),f(1,"svg",8),O(2,"path",20),h()()}if(t&2){let e=S();D("matTooltip",e._intl.firstPageLabel)("matTooltipDisabled",e._previousButtonsDisabled())("disabled",e._previousButtonsDisabled())("tabindex",e._previousButtonsDisabled()?-1:null),q("aria-label",e._intl.firstPageLabel)}}function SB(t,n){if(t&1){let e=Mt();f(0,"button",21),T("click",function(){Te(e);let r=S();return Ae(r._buttonClicked(r.getNumberOfPages()-1,r._nextButtonsDisabled()))}),Gt(),f(1,"svg",8),O(2,"path",22),h()()}if(t&2){let e=S();D("matTooltip",e._intl.lastPageLabel)("matTooltipDisabled",e._nextButtonsDisabled())("disabled",e._nextButtonsDisabled())("tabindex",e._nextButtonsDisabled()?-1:null),q("aria-label",e._intl.lastPageLabel)}}var IB=(()=>{class t{changes=new I;itemsPerPageLabel="Items per page:";nextPageLabel="Next page";previousPageLabel="Previous page";firstPageLabel="First page";lastPageLabel="Last page";getRangeLabel=(e,i,r)=>{if(r==0||i==0)return`0 of ${r}`;r=Math.max(r,0);let o=e*i,s=o<r?Math.min(o+i,r):o+i;return`${o+1} \u2013 ${s} of ${r}`};static \u0275fac=function(i){return new(i||t)};static \u0275prov=C({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})(),kB=50;var MB=new y("MAT_PAGINATOR_DEFAULT_OPTIONS"),tk=(()=>{class t{_intl=d(IB);_changeDetectorRef=d(ve);_formFieldAppearance;_pageSizeLabelId=d(Ie).getId("mat-paginator-page-size-label-");_intlChanges;_isInitialized=!1;_initializedStream=new Mi(1);color;get pageIndex(){return this._pageIndex}set pageIndex(e){this._pageIndex=Math.max(e||0,0),this._changeDetectorRef.markForCheck()}_pageIndex=0;get length(){return this._length}set length(e){this._length=e||0,this._changeDetectorRef.markForCheck()}_length=0;get pageSize(){return this._pageSize}set pageSize(e){this._pageSize=Math.max(e||0,0),this._updateDisplayedPageSizeOptions()}_pageSize;get pageSizeOptions(){return this._pageSizeOptions}set pageSizeOptions(e){this._pageSizeOptions=(e||[]).map(i=>lt(i,0)),this._updateDisplayedPageSizeOptions()}_pageSizeOptions=[];hidePageSize=!1;showFirstLastButtons=!1;selectConfig={};disabled=!1;page=new U;_displayedPageSizeOptions;initialized=this._initializedStream;constructor(){let e=this._intl,i=d(MB,{optional:!0});if(this._intlChanges=e.changes.subscribe(()=>this._changeDetectorRef.markForCheck()),i){let{pageSize:r,pageSizeOptions:o,hidePageSize:s,showFirstLastButtons:a}=i;r!=null&&(this._pageSize=r),o!=null&&(this._pageSizeOptions=o),s!=null&&(this.hidePageSize=s),a!=null&&(this.showFirstLastButtons=a)}this._formFieldAppearance=i?.formFieldAppearance||"outline"}ngOnInit(){this._isInitialized=!0,this._updateDisplayedPageSizeOptions(),this._initializedStream.next()}ngOnDestroy(){this._initializedStream.complete(),this._intlChanges.unsubscribe()}nextPage(){this.hasNextPage()&&this._navigate(this.pageIndex+1)}previousPage(){this.hasPreviousPage()&&this._navigate(this.pageIndex-1)}firstPage(){this.hasPreviousPage()&&this._navigate(0)}lastPage(){this.hasNextPage()&&this._navigate(this.getNumberOfPages()-1)}hasPreviousPage(){return this.pageIndex>=1&&this.pageSize!=0}hasNextPage(){let e=this.getNumberOfPages()-1;return this.pageIndex<e&&this.pageSize!=0}getNumberOfPages(){return this.pageSize?Math.ceil(this.length/this.pageSize):0}_changePageSize(e){let i=this.pageIndex*this.pageSize,r=this.pageIndex;this.pageIndex=Math.floor(i/e)||0,this.pageSize=e,this._emitPageEvent(r)}_nextButtonsDisabled(){return this.disabled||!this.hasNextPage()}_previousButtonsDisabled(){return this.disabled||!this.hasPreviousPage()}_updateDisplayedPageSizeOptions(){this._isInitialized&&(this.pageSize||(this._pageSize=this.pageSizeOptions.length!=0?this.pageSizeOptions[0]:kB),this._displayedPageSizeOptions=this.pageSizeOptions.slice(),this._displayedPageSizeOptions.indexOf(this.pageSize)===-1&&this._displayedPageSizeOptions.push(this.pageSize),this._displayedPageSizeOptions.sort((e,i)=>e-i),this._changeDetectorRef.markForCheck())}_emitPageEvent(e){this.page.emit({previousPageIndex:e,pageIndex:this.pageIndex,pageSize:this.pageSize,length:this.length})}_navigate(e){let i=this.pageIndex;e!==i&&(this.pageIndex=e,this._emitPageEvent(i))}_buttonClicked(e,i){i||this._navigate(e)}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=E({type:t,selectors:[["mat-paginator"]],hostAttrs:["role","group",1,"mat-mdc-paginator"],inputs:{color:"color",pageIndex:[2,"pageIndex","pageIndex",lt],length:[2,"length","length",lt],pageSize:[2,"pageSize","pageSize",lt],pageSizeOptions:"pageSizeOptions",hidePageSize:[2,"hidePageSize","hidePageSize",B],showFirstLastButtons:[2,"showFirstLastButtons","showFirstLastButtons",B],selectConfig:"selectConfig",disabled:[2,"disabled","disabled",B]},outputs:{page:"page"},exportAs:["matPaginator"],decls:14,vars:14,consts:[["selectRef",""],[1,"mat-mdc-paginator-outer-container"],[1,"mat-mdc-paginator-container"],[1,"mat-mdc-paginator-page-size"],[1,"mat-mdc-paginator-range-actions"],["aria-atomic","true","aria-live","polite","role","status",1,"mat-mdc-paginator-range-label"],["matIconButton","","type","button","matTooltipPosition","above","disabledInteractive","",1,"mat-mdc-paginator-navigation-first",3,"matTooltip","matTooltipDisabled","disabled","tabindex"],["matIconButton","","type","button","matTooltipPosition","above","disabledInteractive","",1,"mat-mdc-paginator-navigation-previous",3,"click","matTooltip","matTooltipDisabled","disabled","tabindex"],["viewBox","0 0 24 24","focusable","false","aria-hidden","true",1,"mat-mdc-paginator-icon"],["d","M15.41 7.41L14 6l-6 6 6 6 1.41-1.41L10.83 12z"],["matIconButton","","type","button","matTooltipPosition","above","disabledInteractive","",1,"mat-mdc-paginator-navigation-next",3,"click","matTooltip","matTooltipDisabled","disabled","tabindex"],["d","M10 6L8.59 7.41 13.17 12l-4.58 4.59L10 18l6-6z"],["matIconButton","","type","button","matTooltipPosition","above","disabledInteractive","",1,"mat-mdc-paginator-navigation-last",3,"matTooltip","matTooltipDisabled","disabled","tabindex"],["aria-hidden","true",1,"mat-mdc-paginator-page-size-label"],[1,"mat-mdc-paginator-page-size-select",3,"appearance","color"],[1,"mat-mdc-paginator-page-size-value"],["hideSingleSelectionIndicator","",3,"selectionChange","value","disabled","aria-labelledby","panelClass","disableOptionCentering"],[3,"value"],[1,"mat-mdc-paginator-touch-target",3,"click"],["matIconButton","","type","button","matTooltipPosition","above","disabledInteractive","",1,"mat-mdc-paginator-navigation-first",3,"click","matTooltip","matTooltipDisabled","disabled","tabindex"],["d","M18.41 16.59L13.82 12l4.59-4.59L17 6l-6 6 6 6zM6 6h2v12H6z"],["matIconButton","","type","button","matTooltipPosition","above","disabledInteractive","",1,"mat-mdc-paginator-navigation-last",3,"click","matTooltip","matTooltipDisabled","disabled","tabindex"],["d","M5.59 7.41L10.18 12l-4.59 4.59L7 18l6-6-6-6zM16 6h2v12h-2z"]],template:function(i,r){i&1&&(f(0,"div",1)(1,"div",2),N(2,DB,5,4,"div",3),f(3,"div",4)(4,"div",5),_(5),h(),N(6,EB,3,5,"button",6),f(7,"button",7),T("click",function(){return r._buttonClicked(r.pageIndex-1,r._previousButtonsDisabled())}),Gt(),f(8,"svg",8),O(9,"path",9),h()(),Vi(),f(10,"button",10),T("click",function(){return r._buttonClicked(r.pageIndex+1,r._nextButtonsDisabled())}),Gt(),f(11,"svg",8),O(12,"path",11),h()(),N(13,SB,3,5,"button",12),h()()()),i&2&&(p(2),P(r.hidePageSize?-1:2),p(3),we(" ",r._intl.getRangeLabel(r.pageIndex,r.pageSize,r.length)," "),p(),P(r.showFirstLastButtons?6:-1),p(),D("matTooltip",r._intl.previousPageLabel)("matTooltipDisabled",r._previousButtonsDisabled())("disabled",r._previousButtonsDisabled())("tabindex",r._previousButtonsDisabled()?-1:null),q("aria-label",r._intl.previousPageLabel),p(3),D("matTooltip",r._intl.nextPageLabel)("matTooltipDisabled",r._nextButtonsDisabled())("disabled",r._nextButtonsDisabled())("tabindex",r._nextButtonsDisabled()?-1:null),q("aria-label",r._intl.nextPageLabel),p(3),P(r.showFirstLastButtons?13:-1))},dependencies:[kr,KI,sh,$l,ek],styles:[`.mat-mdc-paginator {
  display: block;
  -moz-osx-font-smoothing: grayscale;
  -webkit-font-smoothing: antialiased;
  color: var(--mat-paginator-container-text-color, var(--mat-sys-on-surface));
  background-color: var(--mat-paginator-container-background-color, var(--mat-sys-surface));
  font-family: var(--mat-paginator-container-text-font, var(--mat-sys-body-small-font));
  line-height: var(--mat-paginator-container-text-line-height, var(--mat-sys-body-small-line-height));
  font-size: var(--mat-paginator-container-text-size, var(--mat-sys-body-small-size));
  font-weight: var(--mat-paginator-container-text-weight, var(--mat-sys-body-small-weight));
  letter-spacing: var(--mat-paginator-container-text-tracking, var(--mat-sys-body-small-tracking));
  --mat-form-field-container-height: var(--mat-paginator-form-field-container-height, 40px);
  --mat-form-field-container-vertical-padding: var(--mat-paginator-form-field-container-vertical-padding, 8px);
}
.mat-mdc-paginator .mat-mdc-select-value {
  font-size: var(--mat-paginator-select-trigger-text-size, var(--mat-sys-body-small-size));
}
.mat-mdc-paginator .mat-mdc-form-field-subscript-wrapper {
  display: none;
}
.mat-mdc-paginator .mat-mdc-select {
  line-height: 1.5;
}

.mat-mdc-paginator-outer-container {
  display: flex;
}

.mat-mdc-paginator-container {
  display: flex;
  align-items: center;
  justify-content: flex-end;
  padding: 0 8px;
  flex-wrap: wrap;
  width: 100%;
  min-height: var(--mat-paginator-container-size, 56px);
}

.mat-mdc-paginator-page-size {
  display: flex;
  align-items: baseline;
  margin-right: 8px;
}
[dir=rtl] .mat-mdc-paginator-page-size {
  margin-right: 0;
  margin-left: 8px;
}

.mat-mdc-paginator-page-size-label {
  margin: 0 4px;
}

.mat-mdc-paginator-page-size-select {
  margin: 0 4px;
  width: var(--mat-paginator-page-size-select-width, 84px);
}

.mat-mdc-paginator-range-label {
  margin: 0 32px 0 24px;
}

.mat-mdc-paginator-range-actions {
  display: flex;
  align-items: center;
}

.mat-mdc-paginator-icon {
  display: inline-block;
  width: 28px;
  fill: var(--mat-paginator-enabled-icon-color, var(--mat-sys-on-surface-variant));
}
.mat-mdc-icon-button[aria-disabled] .mat-mdc-paginator-icon {
  fill: var(--mat-paginator-disabled-icon-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
}
[dir=rtl] .mat-mdc-paginator-icon {
  transform: rotate(180deg);
}

@media (forced-colors: active) {
  .mat-mdc-icon-button[aria-disabled] .mat-mdc-paginator-icon,
  .mat-mdc-paginator-icon {
    fill: currentColor;
  }
  .mat-mdc-paginator-range-actions .mat-mdc-icon-button {
    outline: solid 1px;
  }
  .mat-mdc-paginator-range-actions .mat-mdc-icon-button[aria-disabled] {
    color: GrayText;
  }
}
.mat-mdc-paginator-touch-target {
  display: var(--mat-paginator-touch-target-display, block);
  position: absolute;
  top: 50%;
  left: 50%;
  width: var(--mat-paginator-page-size-select-width, 84px);
  height: var(--mat-paginator-page-size-select-touch-target-height, 48px);
  background-color: transparent;
  transform: translate(-50%, -50%);
  cursor: pointer;
}
`],encapsulation:2,changeDetection:0})}return t})();var TB=(t,n)=>n.id;function AB(t,n){if(t&1&&O(0,"app-product-item",12),t&2){let e=n.$implicit;D("product",e)}}function RB(t,n){if(t&1&&(f(0,"mat-list-option",14),_(1),h()),t&2){let e=n.$implicit,i=S(2);D("value",e.value)("selected",i.shopParams.sort===e.value),p(),we(" ",e.label," ")}}function OB(t,n){if(t&1){let e=Mt();f(0,"div",2)(1,"div",3)(2,"mat-paginator",4),T("page",function(r){Te(e);let o=S();return Ae(o.handlePageEvent(r))}),h(),f(3,"form",5,0),T("ngSubmit",function(){Te(e);let r=S();return Ae(r.onSearchChange())}),f(5,"input",6),Jr("ngModelChange",function(r){Te(e);let o=S();return is(o.shopParams.search,r)||(o.shopParams.search=r),Ae(r)}),h(),f(6,"button",7)(7,"mat-icon"),_(8,"search"),h()()(),f(9,"div",8)(10,"button",9),T("click",function(){Te(e);let r=S();return Ae(r.openFiltersDialog())}),f(11,"mat-icon"),_(12,"filter_list"),h(),_(13," Filters "),h(),f(14,"button",10)(15,"mat-icon"),_(16,"swap_vert"),h(),_(17," Sort "),h()()(),f(18,"div",11),tt(19,AB,1,1,"app-product-item",12,TB),h()(),f(21,"mat-menu",null,1)(23,"mat-selection-list",13),T("selectionChange",function(r){Te(e);let o=S();return Ae(o.onSortChange(r))}),tt(24,RB,2,3,"mat-list-option",14,vi),h()()}if(t&2){let e=n,i=Re(22),r=S();p(2),D("length",e.count)("pageSize",r.shopParams.pageSize)("showFirstLastButtons",!0)("pageSizeOptions",r.pageSizeOptions)("pageIndex",r.shopParams.pageIndex-1),p(3),Kr("ngModel",r.shopParams.search),p(9),D("matMenuTriggerFor",i),p(5),nt(e.data),p(4),D("multiple",!1),p(),nt(r.sortOptions)}}var ah=class t{shopService=d(so);dialogService=d(iI);products=M(null);sortOptions=[{value:"nameAsc",label:"Name (A-Z)"},{value:"priceAsc",label:"Price (Low to High)"},{value:"priceDesc",label:"Price (High to Low)"}];pageSizeOptions=[5,10,15,20];shopParams=new rh;ngOnInit(){this.initialiseShop()}initialiseShop(){this.shopService.getGenres(),this.getProducts()}getProducts(){this.shopService.getProducts(this.shopParams).subscribe({next:n=>this.products.set(n),error:n=>console.error(n)})}handlePageEvent(n){this.shopParams.pageIndex=n.pageIndex+1,this.shopParams.pageSize=n.pageSize,this.getProducts()}onSearchChange(){this.shopParams.pageIndex=1,this.getProducts()}onSortChange(n){let e=n.options[0];e&&(this.shopParams.sort=e.value,this.shopParams.pageIndex=1,this.getProducts())}openFiltersDialog(){this.dialogService.open(th,{minWidth:"500px",autoFocus:!1,data:{selectedGenres:this.shopParams.genres}}).afterClosed().subscribe({next:e=>{e&&(this.shopParams.genres=e.selectedGenres,this.shopParams.pageIndex=1,this.getProducts())}})}static \u0275fac=function(e){return new(e||t)};static \u0275cmp=E({type:t,selectors:[["app-shop"]],decls:1,vars:1,consts:[["searchForm","ngForm"],["sortMenu","matMenu"],[1,"flex","flex-col","gap-3"],[1,"flex","justify-between"],["aria-label","Select page",3,"page","length","pageSize","showFirstLastButtons","pageSizeOptions","pageIndex"],[1,"relative","flex","items-center","w-full","max-w-md","mx-4",3,"ngSubmit"],["type","text","name","search","placeholder","Search name or artist...",1,"w-full","pl-4","pr-12","py-3","text-sm","border","border-gray-300","rounded-lg","focus:outline-none","focus:ring-2","focus:ring-blue-500",3,"ngModelChange","ngModel"],["mat-icon-button","","type","submit",1,"absolute","right-4","flex","items-center","cursor-pointer"],[1,"flex","gap-3","items-center"],["mat-stroked-button","",3,"click"],["mat-stroked-button","",3,"matMenuTriggerFor"],[1,"grid","grid-cols-5","gap-4"],[3,"product"],[3,"selectionChange","multiple"],[3,"value","selected"]],template:function(e,i){if(e&1&&N(0,OB,26,8),e&2){let r;P((r=i.products())?0:-1,r)}},dependencies:[Af,Pe,wt,po,ih,ld,cd,tk,Ua,Ha,ho,ii,Ba,xs,ys],encapsulation:2})};var NB=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275cmp=E({type:t,selectors:[["ng-component"]],hostAttrs:["cdk-text-field-style-loader",""],decls:0,vars:0,template:function(i,r){},styles:[`textarea.cdk-textarea-autosize {
  resize: none;
}

textarea.cdk-textarea-autosize-measuring {
  padding: 2px 0 !important;
  box-sizing: content-box !important;
  height: auto !important;
  overflow: hidden !important;
}

textarea.cdk-textarea-autosize-measuring-firefox {
  padding: 2px 0 !important;
  box-sizing: content-box !important;
  height: 0 !important;
}

@keyframes cdk-text-field-autofill-start { /*!*/ }
@keyframes cdk-text-field-autofill-end { /*!*/ }
.cdk-text-field-autofill-monitored:-webkit-autofill {
  animation: cdk-text-field-autofill-start 0s 1ms;
}

.cdk-text-field-autofill-monitored:not(:-webkit-autofill) {
  animation: cdk-text-field-autofill-end 0s 1ms;
}
`],encapsulation:2,changeDetection:0})}return t})(),PB={passive:!0},nk=(()=>{class t{_platform=d(ye);_ngZone=d(H);_renderer=d(Ot).createRenderer(null,null);_styleLoader=d(qe);_monitoredElements=new Map;constructor(){}monitor(e){if(!this._platform.isBrowser)return St;this._styleLoader.load(NB);let i=xn(e),r=this._monitoredElements.get(i);if(r)return r.subject;let o=new I,s="cdk-text-field-autofilled",a=l=>{l.animationName==="cdk-text-field-autofill-start"&&!i.classList.contains(s)?(i.classList.add(s),this._ngZone.run(()=>o.next({target:l.target,isAutofilled:!0}))):l.animationName==="cdk-text-field-autofill-end"&&i.classList.contains(s)&&(i.classList.remove(s),this._ngZone.run(()=>o.next({target:l.target,isAutofilled:!1})))},c=this._ngZone.runOutsideAngular(()=>(i.classList.add("cdk-text-field-autofill-monitored"),this._renderer.listen(i,"animationstart",a,PB)));return this._monitoredElements.set(i,{subject:o,unlisten:c}),o}stopMonitoring(e){let i=xn(e),r=this._monitoredElements.get(i);r&&(r.unlisten(),r.subject.complete(),i.classList.remove("cdk-text-field-autofill-monitored"),i.classList.remove("cdk-text-field-autofilled"),this._monitoredElements.delete(i))}ngOnDestroy(){this._monitoredElements.forEach((e,i)=>this.stopMonitoring(i))}static \u0275fac=function(i){return new(i||t)};static \u0275prov=C({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();var ik=new y("MAT_INPUT_VALUE_ACCESSOR");var FB=["button","checkbox","file","hidden","image","radio","range","reset","submit"],LB=new y("MAT_INPUT_CONFIG"),qa=(()=>{class t{_elementRef=d(L);_platform=d(ye);ngControl=d(Cn,{optional:!0,self:!0});_autofillMonitor=d(nk);_ngZone=d(H);_formField=d(md,{optional:!0});_renderer=d(xe);_uid=d(Ie).getId("mat-input-");_previousNativeValue;_inputValueAccessor;_signalBasedValueAccessor;_previousPlaceholder=null;_errorStateTracker;_config=d(LB,{optional:!0});_cleanupIosKeyup;_cleanupWebkitWheel;_isServer=!1;_isNativeSelect=!1;_isTextarea=!1;_isInFormField=!1;focused=!1;stateChanges=new I;controlType="mat-input";autofilled=!1;get disabled(){return this._disabled}set disabled(e){this._disabled=Rt(e),this.focused&&(this.focused=!1,this.stateChanges.next())}_disabled=!1;get id(){return this._id}set id(e){this._id=e||this._uid}_id;placeholder;name;get required(){return this._required??this.ngControl?.control?.hasValidator(Jt.required)??!1}set required(e){this._required=Rt(e)}_required;get type(){return this._type}set type(e){this._type=e||"text",this._validateType(),!this._isTextarea&&Eb().has(this._type)&&(this._elementRef.nativeElement.type=this._type)}_type="text";get errorStateMatcher(){return this._errorStateTracker.matcher}set errorStateMatcher(e){this._errorStateTracker.matcher=e}userAriaDescribedBy;get value(){return this._signalBasedValueAccessor?this._signalBasedValueAccessor.value():this._inputValueAccessor.value}set value(e){e!==this.value&&(this._signalBasedValueAccessor?this._signalBasedValueAccessor.value.set(e):this._inputValueAccessor.value=e,this.stateChanges.next())}get readonly(){return this._readonly}set readonly(e){this._readonly=Rt(e)}_readonly=!1;disabledInteractive;get errorState(){return this._errorStateTracker.errorState}set errorState(e){this._errorStateTracker.errorState=e}_neverEmptyInputTypes=["date","datetime","datetime-local","month","time","week"].filter(e=>Eb().has(e));constructor(){let e=d(ys,{optional:!0}),i=d(Dr,{optional:!0}),r=d(go),o=d(ik,{optional:!0,self:!0}),s=this._elementRef.nativeElement,a=s.nodeName.toLowerCase();o?$i(o.value)?this._signalBasedValueAccessor=o:this._inputValueAccessor=o:this._inputValueAccessor=s,this._previousNativeValue=this.value,this.id=this.id,this._platform.IOS&&this._ngZone.runOutsideAngular(()=>{this._cleanupIosKeyup=this._renderer.listen(s,"keyup",this._iOSKeyupListener)}),this._errorStateTracker=new Wa(r,this.ngControl,i,e,this.stateChanges),this._isServer=!this._platform.isBrowser,this._isNativeSelect=a==="select",this._isTextarea=a==="textarea",this._isInFormField=!!this._formField,this.disabledInteractive=this._config?.disabledInteractive||!1,this._isNativeSelect&&(this.controlType=s.multiple?"mat-native-select-multiple":"mat-native-select"),this._signalBasedValueAccessor&&fr(()=>{this._signalBasedValueAccessor.value(),this.stateChanges.next()})}ngAfterViewInit(){this._platform.isBrowser&&this._autofillMonitor.monitor(this._elementRef.nativeElement).subscribe(e=>{this.autofilled=e.isAutofilled,this.stateChanges.next()})}ngOnChanges(){this.stateChanges.next()}ngOnDestroy(){this.stateChanges.complete(),this._platform.isBrowser&&this._autofillMonitor.stopMonitoring(this._elementRef.nativeElement),this._cleanupIosKeyup?.(),this._cleanupWebkitWheel?.()}ngDoCheck(){this.ngControl&&(this.updateErrorState(),this.ngControl.disabled!==null&&this.ngControl.disabled!==this.disabled&&(this.disabled=this.ngControl.disabled,this.stateChanges.next())),this._dirtyCheckNativeValue(),this._dirtyCheckPlaceholder()}focus(e){this._elementRef.nativeElement.focus(e)}updateErrorState(){this._errorStateTracker.updateErrorState()}_focusChanged(e){if(e!==this.focused){if(!this._isNativeSelect&&e&&this.disabled&&this.disabledInteractive){let i=this._elementRef.nativeElement;i.type==="number"?(i.type="text",i.setSelectionRange(0,0),i.type="number"):i.setSelectionRange(0,0)}this.focused=e,this.stateChanges.next()}}_onInput(){}_dirtyCheckNativeValue(){let e=this._elementRef.nativeElement.value;this._previousNativeValue!==e&&(this._previousNativeValue=e,this.stateChanges.next())}_dirtyCheckPlaceholder(){let e=this._getPlaceholder();if(e!==this._previousPlaceholder){let i=this._elementRef.nativeElement;this._previousPlaceholder=e,e?i.setAttribute("placeholder",e):i.removeAttribute("placeholder")}}_getPlaceholder(){return this.placeholder||null}_validateType(){FB.indexOf(this._type)>-1}_isNeverEmpty(){return this._neverEmptyInputTypes.indexOf(this._type)>-1}_isBadInput(){let e=this._elementRef.nativeElement.validity;return e&&e.badInput}get empty(){return!this._isNeverEmpty()&&!this._elementRef.nativeElement.value&&!this._isBadInput()&&!this.autofilled}get shouldLabelFloat(){if(this._isNativeSelect){let e=this._elementRef.nativeElement,i=e.options[0];return this.focused||e.multiple||!this.empty||!!(e.selectedIndex>-1&&i&&i.label)}else return this.focused&&!this.disabled||!this.empty}get describedByIds(){return this._elementRef.nativeElement.getAttribute("aria-describedby")?.split(" ")||[]}setDescribedByIds(e){let i=this._elementRef.nativeElement;e.length?i.setAttribute("aria-describedby",e.join(" ")):i.removeAttribute("aria-describedby")}onContainerClick(){this.focused||this.focus()}_isInlineSelect(){let e=this._elementRef.nativeElement;return this._isNativeSelect&&(e.multiple||e.size>1)}_iOSKeyupListener=e=>{let i=e.target;!i.value&&i.selectionStart===0&&i.selectionEnd===0&&(i.setSelectionRange(1,1),i.setSelectionRange(0,0))};_getReadonlyAttribute(){return this._isNativeSelect?null:this.readonly||this.disabled&&this.disabledInteractive?"true":null}static \u0275fac=function(i){return new(i||t)};static \u0275dir=A({type:t,selectors:[["input","matInput",""],["textarea","matInput",""],["select","matNativeControl",""],["input","matNativeControl",""],["textarea","matNativeControl",""]],hostAttrs:[1,"mat-mdc-input-element"],hostVars:21,hostBindings:function(i,r){i&1&&T("focus",function(){return r._focusChanged(!0)})("blur",function(){return r._focusChanged(!1)})("input",function(){return r._onInput()}),i&2&&(mt("id",r.id)("disabled",r.disabled&&!r.disabledInteractive)("required",r.required),q("name",r.name||null)("readonly",r._getReadonlyAttribute())("aria-disabled",r.disabled&&r.disabledInteractive?"true":null)("aria-invalid",r.empty&&r.required?null:r.errorState)("aria-required",r.required)("id",r.id),z("mat-input-server",r._isServer)("mat-mdc-form-field-textarea-control",r._isInFormField&&r._isTextarea)("mat-mdc-form-field-input-control",r._isInFormField)("mat-mdc-input-disabled-interactive",r.disabledInteractive)("mdc-text-field__input",r._isInFormField)("mat-mdc-native-select-inline",r._isInlineSelect()))},inputs:{disabled:"disabled",id:"id",placeholder:"placeholder",name:"name",required:"required",type:"type",errorStateMatcher:"errorStateMatcher",userAriaDescribedBy:[0,"aria-describedby","userAriaDescribedBy"],value:"value",readonly:"readonly",disabledInteractive:[2,"disabledInteractive","disabledInteractive",B]},exportAs:["matInput"],features:[Ee([{provide:ud,useExisting:t}]),ze]})}return t})();function jB(t,n){if(t&1){let e=Mt();f(0,"div",1)(1,"button",2),T("click",function(){Te(e);let r=S();return Ae(r.action())}),_(2),h()()}if(t&2){let e=S();p(2),we(" ",e.data.action," ")}}var BB=["label"];function VB(t,n){}var HB=Math.pow(2,31)-1,fd=class{_overlayRef;instance;containerInstance;_afterDismissed=new I;_afterOpened=new I;_onAction=new I;_durationTimeoutId;_dismissedByAction=!1;constructor(n,e){this._overlayRef=e,this.containerInstance=n,n._onExit.subscribe(()=>this._finishDismiss())}dismiss(){this._afterDismissed.closed||this.containerInstance.exit(),clearTimeout(this._durationTimeoutId)}dismissWithAction(){this._onAction.closed||(this._dismissedByAction=!0,this._onAction.next(),this._onAction.complete(),this.dismiss()),clearTimeout(this._durationTimeoutId)}closeWithAction(){this.dismissWithAction()}_dismissAfter(n){this._durationTimeoutId=setTimeout(()=>this.dismiss(),Math.min(n,HB))}_open(){this._afterOpened.closed||(this._afterOpened.next(),this._afterOpened.complete())}_finishDismiss(){this._overlayRef.dispose(),this._onAction.closed||this._onAction.complete(),this._afterDismissed.next({dismissedByAction:this._dismissedByAction}),this._afterDismissed.complete(),this._dismissedByAction=!1}afterDismissed(){return this._afterDismissed}afterOpened(){return this.containerInstance._onEnter}onAction(){return this._onAction}},rk=new y("MatSnackBarData"),Ya=class{politeness="polite";announcementMessage="";viewContainerRef;duration=0;panelClass;direction;data=null;horizontalPosition="center";verticalPosition="bottom"},UB=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275dir=A({type:t,selectors:[["","matSnackBarLabel",""]],hostAttrs:[1,"mat-mdc-snack-bar-label","mdc-snackbar__label"]})}return t})(),zB=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275dir=A({type:t,selectors:[["","matSnackBarActions",""]],hostAttrs:[1,"mat-mdc-snack-bar-actions","mdc-snackbar__actions"]})}return t})(),$B=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275dir=A({type:t,selectors:[["","matSnackBarAction",""]],hostAttrs:[1,"mat-mdc-snack-bar-action","mdc-snackbar__action"]})}return t})(),GB=(()=>{class t{snackBarRef=d(fd);data=d(rk);constructor(){}action(){this.snackBarRef.dismissWithAction()}get hasAction(){return!!this.data.action}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=E({type:t,selectors:[["simple-snack-bar"]],hostAttrs:[1,"mat-mdc-simple-snack-bar"],exportAs:["matSnackBar"],decls:3,vars:2,consts:[["matSnackBarLabel",""],["matSnackBarActions",""],["matButton","","matSnackBarAction","",3,"click"]],template:function(i,r){i&1&&(f(0,"div",0),_(1),h(),N(2,jB,3,1,"div",1)),i&2&&(p(),we(" ",r.data.message,`
`),p(),P(r.hasAction?2:-1))},dependencies:[Pe,UB,zB,$B],styles:[`.mat-mdc-simple-snack-bar {
  display: flex;
}
.mat-mdc-simple-snack-bar .mat-mdc-snack-bar-label {
  max-height: 50vh;
  overflow: auto;
}
`],encapsulation:2,changeDetection:0})}return t})(),hy="_mat-snack-bar-enter",py="_mat-snack-bar-exit",WB=(()=>{class t extends co{_ngZone=d(H);_elementRef=d(L);_changeDetectorRef=d(ve);_platform=d(ye);_animationsDisabled=ke();snackBarConfig=d(Ya);_document=d(K);_trackedModals=new Set;_enterFallback;_exitFallback;_injector=d(X);_announceDelay=150;_announceTimeoutId;_destroyed=!1;_portalOutlet;_onAnnounce=new I;_onExit=new I;_onEnter=new I;_animationState="void";_live;_label;_role;_liveElementId=d(Ie).getId("mat-snack-bar-container-live-");constructor(){super();let e=this.snackBarConfig;e.politeness==="assertive"&&!e.announcementMessage?this._live="assertive":e.politeness==="off"?this._live="off":this._live="polite",this._platform.FIREFOX&&(this._live==="polite"&&(this._role="status"),this._live==="assertive"&&(this._role="alert"))}attachComponentPortal(e){this._assertNotAttached();let i=this._portalOutlet.attachComponentPortal(e);return this._afterPortalAttached(),i}attachTemplatePortal(e){this._assertNotAttached();let i=this._portalOutlet.attachTemplatePortal(e);return this._afterPortalAttached(),i}attachDomPortal=e=>{this._assertNotAttached();let i=this._portalOutlet.attachDomPortal(e);return this._afterPortalAttached(),i};onAnimationEnd(e){e===py?this._completeExit():e===hy&&(clearTimeout(this._enterFallback),this._ngZone.run(()=>{this._onEnter.next(),this._onEnter.complete()}))}enter(){this._destroyed||(this._animationState="visible",this._changeDetectorRef.markForCheck(),this._changeDetectorRef.detectChanges(),this._screenReaderAnnounce(),this._animationsDisabled?bt(()=>{this._ngZone.run(()=>queueMicrotask(()=>this.onAnimationEnd(hy)))},{injector:this._injector}):(clearTimeout(this._enterFallback),this._enterFallback=setTimeout(()=>{this._elementRef.nativeElement.classList.add("mat-snack-bar-fallback-visible"),this.onAnimationEnd(hy)},200)))}exit(){return this._destroyed?W(void 0):(this._ngZone.run(()=>{this._animationState="hidden",this._changeDetectorRef.markForCheck(),this._elementRef.nativeElement.setAttribute("mat-exit",""),clearTimeout(this._announceTimeoutId),this._animationsDisabled?bt(()=>{this._ngZone.run(()=>queueMicrotask(()=>this.onAnimationEnd(py)))},{injector:this._injector}):(clearTimeout(this._exitFallback),this._exitFallback=setTimeout(()=>this.onAnimationEnd(py),200))}),this._onExit)}ngOnDestroy(){this._destroyed=!0,this._clearFromModals(),this._completeExit()}_completeExit(){clearTimeout(this._exitFallback),queueMicrotask(()=>{this._onExit.next(),this._onExit.complete()})}_afterPortalAttached(){let e=this._elementRef.nativeElement,i=this.snackBarConfig.panelClass;i&&(Array.isArray(i)?i.forEach(s=>e.classList.add(s)):e.classList.add(i)),this._exposeToModals();let r=this._label.nativeElement,o="mdc-snackbar__label";r.classList.toggle(o,!r.querySelector(`.${o}`))}_exposeToModals(){let e=this._liveElementId,i=this._document.querySelectorAll('body > .cdk-overlay-container [aria-modal="true"]');for(let r=0;r<i.length;r++){let o=i[r],s=o.getAttribute("aria-owns");this._trackedModals.add(o),s?s.indexOf(e)===-1&&o.setAttribute("aria-owns",s+" "+e):o.setAttribute("aria-owns",e)}}_clearFromModals(){this._trackedModals.forEach(e=>{let i=e.getAttribute("aria-owns");if(i){let r=i.replace(this._liveElementId,"").trim();r.length>0?e.setAttribute("aria-owns",r):e.removeAttribute("aria-owns")}}),this._trackedModals.clear()}_assertNotAttached(){this._portalOutlet.hasAttached()}_screenReaderAnnounce(){this._announceTimeoutId||this._ngZone.runOutsideAngular(()=>{this._announceTimeoutId=setTimeout(()=>{if(this._destroyed)return;let e=this._elementRef.nativeElement,i=e.querySelector("[aria-hidden]"),r=e.querySelector("[aria-live]");if(i&&r){let o=null;this._platform.isBrowser&&document.activeElement instanceof HTMLElement&&i.contains(document.activeElement)&&(o=document.activeElement),i.removeAttribute("aria-hidden"),r.appendChild(i),o?.focus(),this._onAnnounce.next(),this._onAnnounce.complete()}},this._announceDelay)})}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=E({type:t,selectors:[["mat-snack-bar-container"]],viewQuery:function(i,r){if(i&1&&Ve(Ki,7)(BB,7),i&2){let o;$(o=G())&&(r._portalOutlet=o.first),$(o=G())&&(r._label=o.first)}},hostAttrs:[1,"mdc-snackbar","mat-mdc-snack-bar-container"],hostVars:6,hostBindings:function(i,r){i&1&&T("animationend",function(s){return r.onAnimationEnd(s.animationName)})("animationcancel",function(s){return r.onAnimationEnd(s.animationName)}),i&2&&z("mat-snack-bar-container-enter",r._animationState==="visible")("mat-snack-bar-container-exit",r._animationState==="hidden")("mat-snack-bar-container-animations-enabled",!r._animationsDisabled)},features:[fe],decls:6,vars:3,consts:[["label",""],[1,"mdc-snackbar__surface","mat-mdc-snackbar-surface"],[1,"mat-mdc-snack-bar-label"],["aria-hidden","true"],["cdkPortalOutlet",""]],template:function(i,r){i&1&&(f(0,"div",1)(1,"div",2,0)(3,"div",3),$e(4,VB,0,0,"ng-template",4),h(),O(5,"div"),h()()),i&2&&(p(5),q("aria-live",r._live)("role",r._role)("id",r._liveElementId))},dependencies:[Ki],styles:[`@keyframes _mat-snack-bar-enter {
  from {
    transform: scale(0.8);
    opacity: 0;
  }
  to {
    transform: scale(1);
    opacity: 1;
  }
}
@keyframes _mat-snack-bar-exit {
  from {
    opacity: 1;
  }
  to {
    opacity: 0;
  }
}
.mat-mdc-snack-bar-container {
  display: flex;
  align-items: center;
  justify-content: center;
  box-sizing: border-box;
  -webkit-tap-highlight-color: rgba(0, 0, 0, 0);
  margin: 8px;
}
.mat-mdc-snack-bar-handset .mat-mdc-snack-bar-container {
  width: 100vw;
}

.mat-snack-bar-container-animations-enabled {
  opacity: 0;
}
.mat-snack-bar-container-animations-enabled.mat-snack-bar-fallback-visible {
  opacity: 1;
}
.mat-snack-bar-container-animations-enabled.mat-snack-bar-container-enter {
  animation: _mat-snack-bar-enter 150ms cubic-bezier(0, 0, 0.2, 1) forwards;
}
.mat-snack-bar-container-animations-enabled.mat-snack-bar-container-exit {
  animation: _mat-snack-bar-exit 75ms cubic-bezier(0.4, 0, 1, 1) forwards;
}

.mat-mdc-snackbar-surface {
  box-shadow: 0px 3px 5px -1px rgba(0, 0, 0, 0.2), 0px 6px 10px 0px rgba(0, 0, 0, 0.14), 0px 1px 18px 0px rgba(0, 0, 0, 0.12);
  display: flex;
  align-items: center;
  justify-content: flex-start;
  box-sizing: border-box;
  padding-left: 0;
  padding-right: 8px;
}
[dir=rtl] .mat-mdc-snackbar-surface {
  padding-right: 0;
  padding-left: 8px;
}
.mat-mdc-snack-bar-container .mat-mdc-snackbar-surface {
  min-width: 344px;
  max-width: 672px;
}
.mat-mdc-snack-bar-handset .mat-mdc-snackbar-surface {
  width: 100%;
  min-width: 0;
}
@media (forced-colors: active) {
  .mat-mdc-snackbar-surface {
    outline: solid 1px;
  }
}
.mat-mdc-snack-bar-container .mat-mdc-snackbar-surface {
  color: var(--mat-snack-bar-supporting-text-color, var(--mat-sys-inverse-on-surface));
  border-radius: var(--mat-snack-bar-container-shape, var(--mat-sys-corner-extra-small));
  background-color: var(--mat-snack-bar-container-color, var(--mat-sys-inverse-surface));
}

.mdc-snackbar__label {
  width: 100%;
  flex-grow: 1;
  box-sizing: border-box;
  margin: 0;
  padding: 14px 8px 14px 16px;
}
[dir=rtl] .mdc-snackbar__label {
  padding-left: 8px;
  padding-right: 16px;
}
.mat-mdc-snack-bar-container .mdc-snackbar__label {
  font-family: var(--mat-snack-bar-supporting-text-font, var(--mat-sys-body-medium-font));
  font-size: var(--mat-snack-bar-supporting-text-size, var(--mat-sys-body-medium-size));
  font-weight: var(--mat-snack-bar-supporting-text-weight, var(--mat-sys-body-medium-weight));
  line-height: var(--mat-snack-bar-supporting-text-line-height, var(--mat-sys-body-medium-line-height));
}

.mat-mdc-snack-bar-actions {
  display: flex;
  flex-shrink: 0;
  align-items: center;
  box-sizing: border-box;
}

.mat-mdc-snack-bar-handset,
.mat-mdc-snack-bar-container,
.mat-mdc-snack-bar-label {
  flex: 1 1 auto;
}

.mat-mdc-snack-bar-container .mat-mdc-button.mat-mdc-snack-bar-action:not(:disabled).mat-unthemed {
  color: var(--mat-snack-bar-button-color, var(--mat-sys-inverse-primary));
}
.mat-mdc-snack-bar-container .mat-mdc-button.mat-mdc-snack-bar-action:not(:disabled) {
  --mat-button-text-state-layer-color: currentColor;
  --mat-button-text-ripple-color: currentColor;
}
.mat-mdc-snack-bar-container .mat-mdc-button.mat-mdc-snack-bar-action:not(:disabled) .mat-ripple-element {
  opacity: 0.1;
}
`],encapsulation:2})}return t})(),qB=new y("mat-snack-bar-default-options",{providedIn:"root",factory:()=>new Ya}),ok=(()=>{class t{_live=d(Pl);_injector=d(X);_breakpointObserver=d(vb);_parentSnackBar=d(t,{optional:!0,skipSelf:!0});_defaultConfig=d(qB);_animationsDisabled=ke();_snackBarRefAtThisLevel=null;simpleSnackBarComponent=GB;snackBarContainerComponent=WB;handsetCssClass="mat-mdc-snack-bar-handset";get _openedSnackBarRef(){let e=this._parentSnackBar;return e?e._openedSnackBarRef:this._snackBarRefAtThisLevel}set _openedSnackBarRef(e){this._parentSnackBar?this._parentSnackBar._openedSnackBarRef=e:this._snackBarRefAtThisLevel=e}constructor(){}openFromComponent(e,i){return this._attach(e,i)}openFromTemplate(e,i){return this._attach(e,i)}open(e,i="",r){let o=b(b({},this._defaultConfig),r);return o.data={message:e,action:i},o.announcementMessage===e&&(o.announcementMessage=void 0),this.openFromComponent(this.simpleSnackBarComponent,o)}dismiss(){this._openedSnackBarRef&&this._openedSnackBarRef.dismiss()}ngOnDestroy(){this._snackBarRefAtThisLevel&&this._snackBarRefAtThisLevel.dismiss()}_attachSnackBarContainer(e,i){let r=i&&i.viewContainerRef&&i.viewContainerRef.injector,o=X.create({parent:r||this._injector,providers:[{provide:Ya,useValue:i}]}),s=new Ci(this.snackBarContainerComponent,i.viewContainerRef,o),a=e.attach(s);return a.instance.snackBarConfig=i,a.instance}_attach(e,i){let r=b(b(b({},new Ya),this._defaultConfig),i),o=this._createOverlay(r),s=this._attachSnackBarContainer(o,r),a=new fd(s,o);if(e instanceof vt){let c=new Un(e,null,{$implicit:r.data,snackBarRef:a});a.instance=s.attachTemplatePortal(c)}else{let c=this._createInjector(r,a),l=new Ci(e,void 0,c),u=s.attachComponentPortal(l);a.instance=u.instance}return this._breakpointObserver.observe(xS.HandsetPortrait).pipe(me(o.detachments())).subscribe(c=>{o.overlayElement.classList.toggle(this.handsetCssClass,c.matches)}),r.announcementMessage&&s._onAnnounce.subscribe(()=>{this._live.announce(r.announcementMessage,r.politeness)}),this._animateSnackBar(a,r),this._openedSnackBarRef=a,this._openedSnackBarRef}_animateSnackBar(e,i){e.afterDismissed().subscribe(()=>{this._openedSnackBarRef==e&&(this._openedSnackBarRef=null),i.announcementMessage&&this._live.clear()}),i.duration&&i.duration>0&&e.afterOpened().subscribe(()=>e._dismissAfter(i.duration)),this._openedSnackBarRef?(this._openedSnackBarRef.afterDismissed().subscribe(()=>{e.containerInstance.enter()}),this._openedSnackBarRef.dismiss()):e.containerInstance.enter()}_createOverlay(e){let i=new wi;i.direction=e.direction;let r=bs(this._injector),o=e.direction==="rtl",s=e.horizontalPosition==="left"||e.horizontalPosition==="start"&&!o||e.horizontalPosition==="end"&&o,a=!s&&e.horizontalPosition!=="center";return s?r.left("0"):a?r.right("0"):r.centerHorizontally(),e.verticalPosition==="top"?r.top("0"):r.bottom("0"),i.positionStrategy=r,i.disableAnimations=this._animationsDisabled,Ji(this._injector,i)}_createInjector(e,i){let r=e&&e.viewContainerRef&&e.viewContainerRef.injector;return X.create({parent:r||this._injector,providers:[{provide:fd,useValue:i},{provide:rk,useValue:e.data}]})}static \u0275fac=function(i){return new(i||t)};static \u0275prov=C({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();var Rn=class t{snackbar=d(ok);error(n){this.snackbar.open(n,"Close",{duration:5e3,panelClass:["snackbar-error"]})}success(n){this.snackbar.open(n,"Close",{duration:5e3,panelClass:["snackbar-success"]})}static \u0275fac=function(e){return new(e||t)};static \u0275prov=C({token:t,factory:t.\u0275fac,providedIn:"root"})};var ch=class t{validate(n){let e=n.value;return e==null||e===""||Number.isInteger(Number(e))?null:{integerOnly:!0}}static \u0275fac=function(e){return new(e||t)};static \u0275dir=A({type:t,selectors:[["","integerOnly",""]],features:[Ee([{provide:Si,useExisting:t,multi:!0}])]})};function YB(t,n){if(t&1){let e=Mt();f(0,"section",1)(1,"div",2)(2,"div",3)(3,"div",4),O(4,"img",5),h(),f(5,"div",6)(6,"h1",7),_(7),h(),f(8,"p",8),_(9),Se(10,"currency"),h(),f(11,"div",9)(12,"button",10),T("click",function(){Te(e);let r=S();return Ae(r.updateCart())}),f(13,"mat-icon"),_(14,"shopping_cart"),h(),_(15),h(),f(16,"mat-form-field",11)(17,"mat-label"),_(18,"Quantity"),h(),f(19,"input",12,0),Jr("ngModelChange",function(r){Te(e);let o=S();return is(o.quantity,r)||(o.quantity=r),Ae(r)}),h()()(),O(21,"mat-divider",13),f(22,"p",14)(23,"span",15),_(24,"Genre:"),h(),_(25),h(),f(26,"p",14)(27,"span",15),_(28,"Label:"),h(),_(29),h(),f(30,"p",14)(31,"span",15),_(32,"Description:"),h(),_(33),h()()()()()}if(t&2){let e=n,i=Re(20),r=S();p(4),D("src",an(e.pictureUrl),Qn),p(3),ia("",e.artist," - ",e.name),p(2),V(Oe(10,11,e.price)),p(3),D("disabled",r.quantity()==r.quantityInCart()||i.invalid),p(3),we(" ",r.getButtonText()," "),p(4),Kr("ngModel",r.quantity),p(6),we(" ",e.genre," "),p(4),we(" ",e.label," "),p(4),we(" ",e.description," ")}}var lh=class t{shopService=d(so);activatedRoute=d(yn);cartService=d(rt);product=M(null);quantityInCart=M(0);quantity=M(1);snackbar=d(Rn);ngOnInit(){this.loadProduct()}loadProduct(){let n=this.activatedRoute.snapshot.paramMap.get("id");n&&this.shopService.getProduct(+n).subscribe({next:e=>{this.product.set(e),this.updateQuantityInCart()},error:e=>console.error(e)})}updateCart(){if(this.product()){if(this.quantity()>this.quantityInCart()){let n=this.quantity()-this.quantityInCart();this.quantityInCart.set(this.quantity()),this.cartService.addItemToCart(this.product(),n),this.snackbar.success(`Updated quantity to ${this.quantityInCart()} for this record`)}else if(this.quantity()<this.quantityInCart()){let n=this.quantityInCart()-this.quantity();this.quantityInCart.set(this.quantity()),this.cartService.removeItemFromCart(this.product().id,n),this.quantity()===0?(this.quantity.set(1),this.snackbar.success("Removed this record from your cart")):this.snackbar.success(`Updated quantity to ${this.quantityInCart()} for this record`)}}}updateQuantityInCart(){this.quantityInCart.set(this.cartService.cart()?.items.find(n=>n.productId===this.product()?.id)?.quantity??0),this.quantityInCart()>0&&this.quantity.set(this.quantityInCart())}getButtonText(){return this.quantityInCart()>0?"Update Cart":"Add to Cart"}static \u0275fac=function(e){return new(e||t)};static \u0275cmp=E({type:t,selectors:[["app-product-details"]],decls:1,vars:1,consts:[["quantityModel","ngModel"],[1,"py-8"],[1,"max-w-6xl","px-4","mx-auto"],[1,"grid","grid-cols-3","gap-8"],[1,"col-span-1"],["alt","product image",1,"w-full","object-cover",3,"src"],[1,"col-span-2","flex","flex-col","gap-4"],[1,"text-2xl","font-semibold"],[1,"text-2xl","font-extrabold"],[1,"flex","gap-4"],["mat-flat-button","",1,"min-h-14",3,"click","disabled"],["appearance","outline","subscriptSizing","dynamic"],["matInput","","type","number","min","0","required","","integerOnly","",3,"ngModelChange","ngModel"],[1,"my-6"],[1,"text-gray-500"],[1,"font-semibold"]],template:function(e,i){if(e&1&&N(0,YB,34,13,"section",1),e&2){let r;P((r=i.product())?0:-1,r)}},dependencies:[Pe,wt,kr,qa,Ir,Cs,Ua,ho,Kb,ii,ny,ty,xs,ch,Ut],encapsulation:2})};function ZB(t,n){if(t&1&&(f(0,"li",4),_(1),h()),t&2){let e=n.$implicit;p(),V(e)}}function QB(t,n){if(t&1&&(f(0,"div",2)(1,"ul",3),tt(2,ZB,2,1,"li",4,vi),h()()),t&2){let e=S();p(2),nt(e.validationErrors())}}var dh=class t{baseUrl=oo.apiUrl;http=d(Wt);validationErrors=M(null);get404Error(){this.http.get(this.baseUrl+"buggy/notfound").subscribe({next:n=>console.log(n),error:n=>console.error(n)})}get500Error(){this.http.get(this.baseUrl+"buggy/internalerror").subscribe({next:n=>console.log(n),error:n=>console.error(n)})}get400Error(){this.http.get(this.baseUrl+"buggy/badrequest").subscribe({next:n=>console.log(n),error:n=>console.error(n)})}get401Error(){this.http.get(this.baseUrl+"buggy/unauthorized").subscribe({next:n=>console.log(n),error:n=>console.error(n)})}get400ValidationError(){this.http.post(this.baseUrl+"buggy/validationerror",{}).subscribe({next:n=>console.log(n),error:n=>{this.validationErrors.set(n),console.error(this.validationErrors())}})}static \u0275fac=function(e){return new(e||t)};static \u0275cmp=E({type:t,selectors:[["app-test-error"]],decls:14,vars:1,consts:[[1,"mt-5","flex","flex-col","items-center","justify-around","gap-4"],["mat-stroked-button","",3,"click"],[1,"mx-auto","max-w-lg","mt-5","bg-red-100"],[1,"space-y-2","p-2"],[1,"text-red-800"]],template:function(e,i){e&1&&(f(0,"div",0)(1,"h2"),_(2,"Test Error Pages"),h(),f(3,"button",1),T("click",function(){return i.get404Error()}),_(4,"Test 404 Error"),h(),f(5,"button",1),T("click",function(){return i.get500Error()}),_(6,"Test 500 Error"),h(),f(7,"button",1),T("click",function(){return i.get400Error()}),_(8,"Test 400 Error"),h(),f(9,"button",1),T("click",function(){return i.get401Error()}),_(10,"Test 401 Error"),h(),f(11,"button",1),T("click",function(){return i.get400ValidationError()}),_(12,"Test 400 Validation Error"),h()(),N(13,QB,4,0,"div",2)),e&2&&(p(13),P(i.validationErrors()?13:-1))},dependencies:[Pe],encapsulation:2})};var uh=class t{static \u0275fac=function(e){return new(e||t)};static \u0275cmp=E({type:t,selectors:[["app-not-found"]],decls:10,vars:0,consts:[[1,"flex","items-center","justify-center","min-h-96"],[1,"text-center"],[1,"text-blue-500!","icon-display"],[1,"text-4xl","font-bold","mt-4"],[1,"text-gray-500","text-lg","mt-2"],["routerLink","/","mat-flat-button","",1,"mt-4"]],template:function(e,i){e&1&&(f(0,"div",0)(1,"div",1)(2,"mat-icon",2),_(3,"error_outline"),h(),f(4,"h1",3),_(5,"404"),h(),f(6,"p",4),_(7,"The page you are looking for does not exist"),h(),f(8,"button",5),_(9,"Back to shop"),h()()())},dependencies:[wt,Pe,dt],styles:[".icon-display[_ngcontent-%COMP%]{transform:scale(2.5)}"]})};function XB(t,n){if(t&1&&(f(0,"h5",2),_(1),h(),f(2,"p",3),_(3,"This error comes from the server, not Angular"),h(),f(4,"p",4),_(5,"What to do next?"),h(),f(6,"ol",5)(7,"li",6),_(8,"Check the network tab in chrome dev tools"),h(),f(9,"li",6),_(10,"Reproduce the error in postman. If same error occurs, don't waste time troubleshooting angular code"),h()(),f(11,"h5",7),_(12,"Stack trace"),h(),f(13,"mat-card",8)(14,"code",9),_(15),h()()),t&2){let e=S();p(),we("Error: ",e.error.message),p(14),V(e.error.details)}}var mh=class t{constructor(n){this.router=n;let e=this.router.currentNavigation();this.error=e?.extras.state?.error}error;static \u0275fac=function(e){return new(e||t)(ee(Xe))};static \u0275cmp=E({type:t,selectors:[["app-server-error"]],decls:4,vars:1,consts:[[1,"container","p-4"],[1,"text-2xl","font-semibold","mb-4"],[1,"text-red-600","mb-2"],[1,"font-bold","mb-2"],[1,"mb-2"],[1,"list-decimal","ml-6","mb-4"],[1,"mb-1"],[1,"text-lg","font-semibold","mb-2"],[1,"p-4"],[1,"whitespace-pre-wrap","wrap-break-word"]],template:function(e,i){e&1&&(f(0,"div",0)(1,"h1",1),_(2,"Internal server error"),h(),N(3,XB,16,2),h()),e&2&&(p(3),P(i.error?3:-1))},dependencies:[Zi],encapsulation:2})};var fh=class t{item=sa.required();cartService=d(rt);snackbar=d(Rn);incrementQuantity(){this.cartService.addItemToCart(this.item())}decrementQuantity(){this.cartService.removeItemFromCart(this.item().productId)}removeItemFromCart(){this.cartService.removeItemFromCart(this.item().productId,this.item().quantity)}static \u0275fac=function(e){return new(e||t)};static \u0275cmp=E({type:t,selectors:[["app-cart-item"]],inputs:{item:[1,"item"]},decls:28,vars:12,consts:[[1,"rounded-lg","border","border-gray-200","shadow-sm","p-4","mb-4"],[1,"flex","justify-between","items-center","gap-6"],[1,"shrink",3,"routerLink"],["alt","product image",1,"h-24","w-24","object-cover",3,"src"],[1,"w-full","flex-1","flex","flex-col"],[1,"font-medium",3,"routerLink"],[1,"text-gray-500"],[1,"mt-2"],["mat-flat-button","",1,"text-white!","bg-red-800!",3,"click"],[1,"flex","items-center","justify-between"],[1,"flex","items-center","gap-3"],["mat-icon-button","",3,"click"],[1,"text-red-600!"],[1,"font-semibold","text-xl"],[1,"text-green-600!"],[1,"text-end","w-32"],[1,"font-bold","text-xl"]],template:function(e,i){e&1&&(f(0,"div",0)(1,"div",1)(2,"a",2),O(3,"img",3),h(),f(4,"div",4)(5,"a",5),_(6),h(),f(7,"p",6),_(8),h(),f(9,"div",7)(10,"button",8),T("click",function(){return i.removeItemFromCart()}),f(11,"mat-icon"),_(12,"delete"),h(),_(13," Delete "),h()()(),f(14,"div",9)(15,"div",10)(16,"button",11),T("click",function(){return i.decrementQuantity()}),f(17,"mat-icon",12),_(18,"remove"),h()(),f(19,"div",13),_(20),h(),f(21,"button",11),T("click",function(){return i.incrementQuantity()}),f(22,"mat-icon",14),_(23,"add"),h()()(),f(24,"div",15)(25,"p",16),_(26),Se(27,"currency"),h()()()()()),e&2&&(p(2),D("routerLink",Ln("/shop/",i.item().productId)),p(),D("src",an(i.item().pictureUrl),Qn),p(2),D("routerLink",Ln("/shop/",i.item().productId)),p(),V(i.item().productName),p(2),V(i.item().artist),p(12),V(i.item().quantity),p(6),V(Oe(27,10,i.item().price)))},dependencies:[dt,Pe,wt,$l,Ut],encapsulation:2})};function KB(t,n){t&1&&(f(0,"div",9)(1,"button",15),_(2,"Checkout"),h(),f(3,"button",16),_(4,"Continue Shopping"),h()())}var Za=class t{cartService=d(rt);location=d(Gi);static \u0275fac=function(e){return new(e||t)};static \u0275cmp=E({type:t,selectors:[["app-order-summary"]],decls:40,vars:13,consts:[[1,"max-w-4xl","space-y-6","w-full"],[1,"space-y-4","rounded-lg","border","border-gray-200","shadow-sm","p-4"],[1,"text-xl","font-semibold"],[1,"space-y-2"],[1,"flex","items-center","justify-between","gap-4"],[1,"font-medium","text-gray-500"],[1,"font-medium"],[1,"font-medium","text-green-600"],[1,"flex","items-center","justify-between","gap-4","border-t","border-gray-200","pt-2"],[1,"flex","flex-col","gap-2"],[1,"rounded-lg","border","border-gray-200","shadow-sm","p-4"],[1,"space-y-2","flex","flex-col"],["appearance","outline","subscriptSizing","dynamic",1,"mb-4"],["matInput","","type","text"],["mat-flat-button",""],["routerLink","/checkout","mat-flat-button",""],["routerLink","/shop","mat-stroked-button",""]],template:function(e,i){if(e&1&&(f(0,"div",0)(1,"div",1)(2,"p",2),_(3,"Order Summary"),h(),f(4,"div",3)(5,"dl",4)(6,"dt",5),_(7,"Subtotal"),h(),f(8,"dd",6),_(9),Se(10,"currency"),h()(),f(11,"dl",4)(12,"dt",5),_(13,"Discount"),h(),f(14,"dd",7),_(15),Se(16,"currency"),h()(),f(17,"dl",4)(18,"dt",5),_(19,"Shipping"),h(),f(20,"dd",6),_(21),Se(22,"currency"),h()(),f(23,"dl",8)(24,"dt",5),_(25,"Total"),h(),f(26,"dd",6),_(27),Se(28,"currency"),h()()(),N(29,KB,5,0,"div",9),h(),f(30,"div",10)(31,"form",11)(32,"label",6),_(33,"Do you have a voucher?"),h(),f(34,"mat-form-field",12)(35,"mat-label"),_(36,"Voucher Code"),h(),O(37,"input",13),h(),f(38,"button",14),_(39,"Apply Code"),h()()()()),e&2){let r,o,s,a;p(9),V(Oe(10,5,(r=i.cartService.totals())==null?null:r.subtotal)),p(6),we("-",Oe(16,7,(o=i.cartService.totals())==null?null:o.discount)),p(6),V(Oe(22,9,(s=i.cartService.totals())==null?null:s.shipping)),p(6),V(Oe(28,11,(a=i.cartService.totals())==null?null:a.total)),p(2),P(i.location.path()!=="/checkout"?29:-1)}},dependencies:[Pe,dt,kr,qa,Ir,Ut],encapsulation:2})};var hh=class t{static \u0275fac=function(e){return new(e||t)};static \u0275cmp=E({type:t,selectors:[["app-empty-state"]],decls:8,vars:0,consts:[[1,"max-w-6xl","mx-auto","mt-32","px-10","py-12","rounded-lg","shadow-md","w-full"],[1,"flex","flex-col","items-center","justify-center","w-full"],[1,"icon-display","mb-6"],[1,"text-gray-500","text-lg","font-semibold","mb-4"],["mat-flat-button","","routerLink","/shop"]],template:function(e,i){e&1&&(f(0,"div",0)(1,"div",1)(2,"mat-icon",2),_(3,"shopping_cart"),h(),f(4,"p",3),_(5,"Your shopping cart is empty"),h(),f(6,"button",4),_(7,"Continue Shopping"),h()()())},dependencies:[wt,Pe,dt],styles:[".icon-display[_ngcontent-%COMP%]{transform:scale(2.5)}"]})};function JB(t,n){if(t&1&&O(0,"app-cart-item",3),t&2){let e=n.$implicit;D("item",e)}}function eV(t,n){if(t&1&&(f(0,"div",0)(1,"div",1)(2,"div",2),tt(3,JB,1,1,"app-cart-item",3,vi),h(),f(5,"div",4),O(6,"app-order-summary"),h()()()),t&2){let e=S();p(3),nt(e.items)}}function tV(t,n){t&1&&O(0,"app-empty-state")}function nV(t,n){t&1&&N(0,eV,7,0,"div",0)(1,tV,1,0,"app-empty-state"),t&2&&P(n.items.length>0?0:1)}function iV(t,n){t&1&&O(0,"app-empty-state")}var ph=class t{cartService=d(rt);static \u0275fac=function(e){return new(e||t)};static \u0275cmp=E({type:t,selectors:[["app-cart"]],decls:3,vars:1,consts:[[1,"mx-auto","max-w-6xl"],[1,"flex","w-full","gap-6","mt-32"],[1,"w-3/4"],[3,"item"],[1,"w-1/4"]],template:function(e,i){if(e&1&&(f(0,"section"),N(1,nV,2,1)(2,iV,1,0,"app-empty-state"),h()),e&2){let r;p(),P((r=i.cartService.cart())?1:2,r)}},dependencies:[fh,Za,hh],encapsulation:2})};var rV=["*"];function oV(t,n){t&1&&re(0)}var gy=(()=>{class t{_elementRef=d(L);constructor(){}focus(){this._elementRef.nativeElement.focus()}static \u0275fac=function(i){return new(i||t)};static \u0275dir=A({type:t,selectors:[["","cdkStepHeader",""]],hostAttrs:["role","tab"]})}return t})(),_y=(()=>{class t{template=d(vt);constructor(){}static \u0275fac=function(i){return new(i||t)};static \u0275dir=A({type:t,selectors:[["","cdkStepLabel",""]]})}return t})();var ws={NUMBER:"number",EDIT:"edit",DONE:"done",ERROR:"error"},sV=new y("STEPPER_GLOBAL_OPTIONS"),gh=(()=>{class t{_stepperOptions;_stepper=d(Qa);_displayDefaultIndicatorType;stepLabel;_childForms;content;stepControl;get interacted(){return this._interacted()}set interacted(e){this._interacted.set(e)}_interacted=M(!1);interactedStream=new U;label;errorMessage;ariaLabel;ariaLabelledby;get state(){return this._state()}set state(e){this._state.set(e)}_state=M(void 0);get editable(){return this._editable()}set editable(e){this._editable.set(e)}_editable=M(!0);optional=!1;get completed(){let e=this._completedOverride(),i=this._interacted();return e??(i&&(!this.stepControl||this.stepControl.valid))}set completed(e){this._completedOverride.set(e)}_completedOverride=M(null);index=M(-1);isSelected=xt(()=>this._stepper.selectedIndex===this.index());indicatorType=xt(()=>{let e=this.isSelected(),i=this.completed,r=this._state()??ws.NUMBER,o=this._editable();return this._showError()&&this.hasError&&!e?ws.ERROR:this._displayDefaultIndicatorType?!i||e?ws.NUMBER:o?ws.EDIT:ws.DONE:i&&!e?ws.DONE:i&&e?r:o&&e?ws.EDIT:r});isNavigable=xt(()=>{let e=this.isSelected();return this.completed||e||!this._stepper.linear});get hasError(){let e=this._customError();return e??this._getDefaultError()}set hasError(e){this._customError.set(e)}_customError=M(null);_getDefaultError(){return this.interacted&&!!this.stepControl?.invalid}constructor(){let e=d(sV,{optional:!0});this._stepperOptions=e||{},this._displayDefaultIndicatorType=this._stepperOptions.displayDefaultIndicatorType!==!1}select(){this._stepper.selected=this}reset(){this._interacted.set(!1),this._completedOverride()!=null&&this._completedOverride.set(!1),this._customError()!=null&&this._customError.set(!1),this.stepControl&&(this._childForms?.forEach(e=>e.resetForm?.()),this.stepControl.reset())}ngOnChanges(){this._stepper._stateChanged()}_markAsInteracted(){this._interacted()||(this._interacted.set(!0),this.interactedStream.emit(this))}_showError(){return this._stepperOptions.showError??this._customError()!=null}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=E({type:t,selectors:[["cdk-step"]],contentQueries:function(i,r,o){if(i&1&&yt(o,_y,5)(o,Di,5),i&2){let s;$(s=G())&&(r.stepLabel=s.first),$(s=G())&&(r._childForms=s)}},viewQuery:function(i,r){if(i&1&&Ve(vt,7),i&2){let o;$(o=G())&&(r.content=o.first)}},inputs:{stepControl:"stepControl",label:"label",errorMessage:"errorMessage",ariaLabel:[0,"aria-label","ariaLabel"],ariaLabelledby:[0,"aria-labelledby","ariaLabelledby"],state:"state",editable:[2,"editable","editable",B],optional:[2,"optional","optional",B],completed:[2,"completed","completed",B],hasError:[2,"hasError","hasError",B]},outputs:{interactedStream:"interacted"},exportAs:["cdkStep"],features:[ze],ngContentSelectors:rV,decls:1,vars:0,template:function(i,r){i&1&&(Ce(),ta(0,oV,1,0,"ng-template"))},encapsulation:2,changeDetection:0})}return t})(),Qa=(()=>{class t{_dir=d(Xt,{optional:!0});_changeDetectorRef=d(ve);_elementRef=d(L);_destroyed=new I;_keyManager;_steps;steps=new En;_stepHeader;_sortedHeaders=new En;get linear(){return this._linear()}set linear(e){this._linear.set(e)}_linear=M(!1);get selectedIndex(){return this._selectedIndex()}set selectedIndex(e){this._steps?(this._isValidIndex(e),this.selectedIndex!==e&&(this.selected?._markAsInteracted(),!this._anyControlsInvalidOrPending(e)&&(e>=this.selectedIndex||this.steps.toArray()[e].editable)&&this._updateSelectedItemIndex(e))):this._selectedIndex.set(e)}_selectedIndex=M(0);get selected(){return this.steps?this.steps.toArray()[this.selectedIndex]:void 0}set selected(e){this.selectedIndex=e&&this.steps?this.steps.toArray().indexOf(e):-1}selectionChange=new U;selectedIndexChange=new U;_groupId=d(Ie).getId("cdk-stepper-");get orientation(){return this._orientation}set orientation(e){this._orientation=e,this._keyManager&&this._keyManager.withVerticalOrientation(e==="vertical")}_orientation="horizontal";constructor(){}ngAfterContentInit(){this._steps.changes.pipe(et(this._steps),me(this._destroyed)).subscribe(e=>{this.steps.reset(e.filter(i=>i._stepper===this)),this.steps.forEach((i,r)=>i.index.set(r)),this.steps.notifyOnChanges()})}ngAfterViewInit(){if(this._stepHeader.changes.pipe(et(this._stepHeader),me(this._destroyed)).subscribe(e=>{this._sortedHeaders.reset(e.toArray().sort((i,r)=>i._elementRef.nativeElement.compareDocumentPosition(r._elementRef.nativeElement)&Node.DOCUMENT_POSITION_FOLLOWING?-1:1)),this._sortedHeaders.notifyOnChanges()}),this._keyManager=new xr(this._sortedHeaders).withWrap().withHomeAndEnd().withVerticalOrientation(this._orientation==="vertical"),this._keyManager.updateActiveItem(this.selectedIndex),(this._dir?this._dir.change:W()).pipe(et(this._layoutDirection()),me(this._destroyed)).subscribe(e=>this._keyManager?.withHorizontalOrientation(e)),this._keyManager.updateActiveItem(this.selectedIndex),this.steps.changes.subscribe(()=>{this.selected||this._selectedIndex.set(Math.max(this.selectedIndex-1,0))}),this._isValidIndex(this.selectedIndex)||this._selectedIndex.set(0),this.linear&&this.selectedIndex>0){let e=this.steps.toArray().slice(0,this._selectedIndex());for(let i of e)i._markAsInteracted()}}ngOnDestroy(){this._keyManager?.destroy(),this.steps.destroy(),this._sortedHeaders.destroy(),this._destroyed.next(),this._destroyed.complete()}next(){this.selectedIndex=Math.min(this._selectedIndex()+1,this.steps.length-1)}previous(){this.selectedIndex=Math.max(this._selectedIndex()-1,0)}reset(){this._updateSelectedItemIndex(0),this.steps.forEach(e=>e.reset()),this._stateChanged()}_getStepLabelId(e){return`${this._groupId}-label-${e}`}_getStepContentId(e){return`${this._groupId}-content-${e}`}_stateChanged(){this._changeDetectorRef.markForCheck()}_getAnimationDirection(e){let i=e-this._selectedIndex();return i<0?this._layoutDirection()==="rtl"?"next":"previous":i>0?this._layoutDirection()==="rtl"?"previous":"next":"current"}_getFocusIndex(){return this._keyManager?this._keyManager.activeItemIndex:this._selectedIndex()}_updateSelectedItemIndex(e){let i=this.steps.toArray(),r=this._selectedIndex();this.selectionChange.emit({selectedIndex:e,previouslySelectedIndex:r,selectedStep:i[e],previouslySelectedStep:i[r]}),this._keyManager&&(this._containsFocus()?this._keyManager.setActiveItem(e):this._keyManager.updateActiveItem(e)),this._selectedIndex.set(e),this.selectedIndexChange.emit(e),this._stateChanged()}_onKeydown(e){let i=Ct(e),r=e.keyCode,o=this._keyManager;o?.activeItemIndex!=null&&!i&&(r===32||r===13)?(this.selectedIndex=o.activeItemIndex,e.preventDefault()):o?.setFocusOrigin("keyboard").onKeydown(e)}_anyControlsInvalidOrPending(e){return this.linear&&e>=0?this.steps.toArray().slice(0,e).some(i=>{let r=i.stepControl;return(r?r.invalid||r.pending||!i.interacted:!i.completed)&&!i.optional&&!i._completedOverride()}):!1}_layoutDirection(){return this._dir&&this._dir.value==="rtl"?"rtl":"ltr"}_containsFocus(){let e=this._elementRef.nativeElement,i=yr();return e===i||e.contains(i)}_isValidIndex(e){return e>-1&&(!this.steps||e<this.steps.length)}static \u0275fac=function(i){return new(i||t)};static \u0275dir=A({type:t,selectors:[["","cdkStepper",""]],contentQueries:function(i,r,o){if(i&1&&yt(o,gh,5)(o,gy,5),i&2){let s;$(s=G())&&(r._steps=s),$(s=G())&&(r._stepHeader=s)}},inputs:{linear:[2,"linear","linear",B],selectedIndex:[2,"selectedIndex","selectedIndex",lt],selected:"selected",orientation:"orientation"},outputs:{selectionChange:"selectionChange",selectedIndexChange:"selectedIndexChange"},exportAs:["cdkStepper"]})}return t})(),sk=(()=>{class t{_stepper=d(Qa);type="submit";constructor(){}static \u0275fac=function(i){return new(i||t)};static \u0275dir=A({type:t,selectors:[["button","cdkStepperNext",""]],hostVars:1,hostBindings:function(i,r){i&1&&T("click",function(){return r._stepper.next()}),i&2&&mt("type",r.type)},inputs:{type:"type"}})}return t})(),ak=(()=>{class t{_stepper=d(Qa);type="button";constructor(){}static \u0275fac=function(i){return new(i||t)};static \u0275dir=A({type:t,selectors:[["button","cdkStepperPrevious",""]],hostVars:1,hostBindings:function(i,r){i&1&&T("click",function(){return r._stepper.previous()}),i&2&&mt("type",r.type)},inputs:{type:"type"}})}return t})(),ck=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=ae({type:t});static \u0275inj=oe({imports:[Yt]})}return t})();var aV=(t,n,e)=>({index:t,active:n,optional:e});function cV(t,n){if(t&1&&sn(0,2),t&2){let e=S();D("ngTemplateOutlet",e.iconOverrides[e.state])("ngTemplateOutletContext",av(2,aV,e.index,e.active,e.optional))}}function lV(t,n){if(t&1&&(f(0,"span",7),_(1),h()),t&2){let e=S(2);p(),V(e._getDefaultTextForState(e.state))}}function dV(t,n){if(t&1&&(f(0,"span",8),_(1),h()),t&2){let e=S(3);p(),V(e._intl.completedLabel)}}function uV(t,n){if(t&1&&(f(0,"span",8),_(1),h()),t&2){let e=S(3);p(),V(e._intl.editableLabel)}}function mV(t,n){if(t&1&&(N(0,dV,2,1,"span",8)(1,uV,2,1,"span",8),f(2,"mat-icon",7),_(3),h()),t&2){let e=S(2);P(e.state==="done"?0:e.state==="edit"?1:-1),p(3),V(e._getDefaultTextForState(e.state))}}function fV(t,n){if(t&1&&N(0,lV,2,1,"span",7)(1,mV,4,2),t&2){let e,i=S();P((e=i.state)==="number"?0:1)}}function hV(t,n){t&1&&(f(0,"div",4),sn(1,9),h()),t&2&&(p(),D("ngTemplateOutlet",n.template))}function pV(t,n){if(t&1&&(f(0,"div",4),_(1),h()),t&2){let e=S();p(),V(e.label)}}function gV(t,n){if(t&1&&(f(0,"div",5),_(1),h()),t&2){let e=S();p(),V(e._intl.optionalLabel)}}function _V(t,n){if(t&1&&(f(0,"div",6),_(1),h()),t&2){let e=S();p(),V(e.errorMessage)}}var lk=["*"];function vV(t,n){}function bV(t,n){if(t&1&&(re(0),$e(1,vV,0,0,"ng-template",0)),t&2){let e=S();p(),D("cdkPortalOutlet",e._portal)}}var yV=["animatedContainer"],dk=t=>({steps:t}),uk=t=>({step:t});function xV(t,n){t&1&&re(0)}function CV(t,n){if(t&1&&(f(0,"div",5),sn(1,9)(2,6),h()),t&2){let e=S(2),i=Re(6);p(),D("ngTemplateOutlet",e.headerPrefix()),p(),D("ngTemplateOutlet",i)("ngTemplateOutletContext",ra(3,dk,e.steps))}}function wV(t,n){if(t&1&&sn(0,6),t&2){let e=S(2),i=Re(6);D("ngTemplateOutlet",i)("ngTemplateOutletContext",ra(2,dk,e.steps))}}function DV(t,n){if(t&1&&(f(0,"div",10,2),sn(2,9),h()),t&2){let e=n.$implicit,i=n.$index,r=S(2);Tt("mat-horizontal-stepper-content-"+r._getAnimationDirection(i)),D("id",r._getStepContentId(i)),q("aria-labelledby",r._getStepLabelId(i))("inert",r.selectedIndex===i?null:""),p(2),D("ngTemplateOutlet",e.content)}}function EV(t,n){if(t&1&&(f(0,"div",3),N(1,CV,3,5,"div",5)(2,wV,1,4,"ng-container",6),f(3,"div",7),tt(4,DV,3,6,"div",8,ns),h()()),t&2){let e=S();p(),P(e.headerPrefix()?1:2),p(3),nt(e.steps)}}function SV(t,n){if(t&1&&sn(0,9),t&2){let e=S(2);D("ngTemplateOutlet",e.headerPrefix())}}function IV(t,n){if(t&1&&(f(0,"div",11),sn(1,6),f(2,"div",12,2)(4,"div",13)(5,"div",14),sn(6,9),h()()()()),t&2){let e=n.$implicit,i=n.$index,r=n.$index,o=n.$count,s=S(2),a=Re(4);p(),D("ngTemplateOutlet",a)("ngTemplateOutletContext",ra(11,uk,e)),p(),z("mat-stepper-vertical-line",r!==o-1)("mat-vertical-content-container-active",s.selectedIndex===i),q("inert",s.selectedIndex===i?null:"")("aria-label",s.ariaLabel),p(2),D("id",s._getStepContentId(i)),q("aria-labelledby",s._getStepLabelId(i)),p(2),D("ngTemplateOutlet",e.content)}}function kV(t,n){if(t&1&&(f(0,"div",4),N(1,SV,1,1,"ng-container",9),tt(2,IV,7,13,"div",11,ns),h()),t&2){let e=S();p(),P(e.headerPrefix()?1:-1),p(),nt(e.steps)}}function MV(t,n){if(t&1){let e=Mt();f(0,"mat-step-header",15),T("click",function(){let r=Te(e).step;return Ae(r.select())})("keydown",function(r){Te(e);let o=S();return Ae(o._onKeydown(r))}),h()}if(t&2){let e=n.step,i=S();z("mat-horizontal-stepper-header",i.orientation==="horizontal")("mat-vertical-stepper-header",i.orientation==="vertical"),D("tabIndex",i._getFocusIndex()===e.index()?0:-1)("id",i._getStepLabelId(e.index()))("index",e.index())("state",e.indicatorType())("label",e.stepLabel||e.label)("selected",e.isSelected())("active",e.isNavigable())("optional",e.optional)("errorMessage",e.errorMessage)("iconOverrides",i._iconOverrides)("disableRipple",i.disableRipple||!e.isNavigable())("color",e.color||i.color),q("role",i.orientation==="horizontal"?"tab":"button")("aria-posinset",i.orientation==="horizontal"?e.index()+1:null)("aria-setsize",i.orientation==="horizontal"?i.steps.length:null)("aria-selected",i.orientation==="horizontal"?e.isSelected():null)("aria-current",i.orientation==="vertical"&&e.isSelected()?"step":null)("aria-disabled",i.orientation==="vertical"&&e.isSelected()?"true":null)("aria-expanded",i.orientation==="vertical"?e.isSelected():null)("aria-controls",i._getStepContentId(e.index()))("aria-label",e.ariaLabel||null)("aria-labelledby",!e.ariaLabel&&e.ariaLabelledby?e.ariaLabelledby:null)("aria-disabled",e.isNavigable()?null:!0)}}function TV(t,n){t&1&&O(0,"div",17)}function AV(t,n){if(t&1&&(sn(0,6),N(1,TV,1,0,"div",17)),t&2){let e=n.$implicit,i=n.$index,r=n.$count;S(2);let o=Re(4);D("ngTemplateOutlet",o)("ngTemplateOutletContext",ra(3,uk,e)),p(),P(i!==r-1?1:-1)}}function RV(t,n){if(t&1&&(f(0,"div",16),tt(1,AV,2,5,null,null,ns),h()),t&2){let e=n.steps,i=S();q("aria-label",i.ariaLabel),p(),nt(e)}}var vy=(()=>{class t extends _y{static \u0275fac=(()=>{let e;return function(r){return(e||(e=ct(t)))(r||t)}})();static \u0275dir=A({type:t,selectors:[["","matStepLabel",""]],features:[fe]})}return t})(),OV=(()=>{class t{changes=new I;optionalLabel="Optional";completedLabel="Completed";editableLabel="Editable";static \u0275fac=function(i){return new(i||t)};static \u0275prov=C({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})(),by=(()=>{class t extends gy{_intl=d(OV);_focusMonitor=d(Tn);_intlSubscription;state;label;errorMessage;iconOverrides;index;selected=!1;active=!1;optional=!1;disableRipple=!1;color;constructor(){super();let e=d(qe);e.load(An),e.load(Qi);let i=d(ve);this._intlSubscription=this._intl.changes.subscribe(()=>i.markForCheck())}ngAfterViewInit(){this._focusMonitor.monitor(this._elementRef,!0)}ngOnDestroy(){this._intlSubscription.unsubscribe(),this._focusMonitor.stopMonitoring(this._elementRef)}focus(e,i){e?this._focusMonitor.focusVia(this._elementRef,e,i):this._elementRef.nativeElement.focus(i)}_stringLabel(){return this.label instanceof vy?null:this.label}_templateLabel(){return this.label instanceof vy?this.label:null}_getHostElement(){return this._elementRef.nativeElement}_getDefaultTextForState(e){return e=="number"?`${this.index+1}`:e=="edit"?"create":e=="error"?"warning":e}_hasEmptyLabel(){return!this._stringLabel()&&!this._templateLabel()&&!this._hasOptionalLabel()&&!this._hasErrorLabel()}_hasOptionalLabel(){return this.optional&&this.state!=="error"}_hasErrorLabel(){return this.state==="error"}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=E({type:t,selectors:[["mat-step-header"]],hostAttrs:["role","",1,"mat-step-header"],hostVars:4,hostBindings:function(i,r){i&2&&(Tt("mat-"+(r.color||"primary")),z("mat-step-header-empty-label",r._hasEmptyLabel()))},inputs:{state:"state",label:"label",errorMessage:"errorMessage",iconOverrides:"iconOverrides",index:"index",selected:"selected",active:"active",optional:"optional",disableRipple:"disableRipple",color:"color"},features:[fe],decls:10,vars:17,consts:[["matRipple","",1,"mat-step-header-ripple","mat-focus-indicator",3,"matRippleTrigger","matRippleDisabled"],[1,"mat-step-icon-content"],[3,"ngTemplateOutlet","ngTemplateOutletContext"],[1,"mat-step-label"],[1,"mat-step-text-label"],[1,"mat-step-optional"],[1,"mat-step-sub-label-error"],["aria-hidden","true"],[1,"cdk-visually-hidden"],[3,"ngTemplateOutlet"]],template:function(i,r){if(i&1&&(O(0,"div",0),f(1,"div")(2,"div",1),N(3,cV,1,6,"ng-container",2)(4,fV,2,1),h()(),f(5,"div",3),N(6,hV,2,1,"div",4)(7,pV,2,1,"div",4),N(8,gV,2,1,"div",5),N(9,_V,2,1,"div",6),h()),i&2){let o;D("matRippleTrigger",r._getHostElement())("matRippleDisabled",r.disableRipple),p(),Tt(Ln("mat-step-icon-state-",r.state," mat-step-icon")),z("mat-step-icon-selected",r.selected),p(2),P(r.iconOverrides&&r.iconOverrides[r.state]?3:4),p(2),z("mat-step-label-active",r.active)("mat-step-label-selected",r.selected)("mat-step-label-error",r.state=="error"),p(),P((o=r._templateLabel())?6:r._stringLabel()?7:-1,o),p(2),P(r._hasOptionalLabel()?8:-1),p(),P(r._hasErrorLabel()?9:-1)}},dependencies:[Xi,qi,wt],styles:[`.mat-step-header {
  overflow: hidden;
  outline: none;
  cursor: pointer;
  position: relative;
  box-sizing: content-box;
  -webkit-tap-highlight-color: transparent;
}
.mat-step-header:focus-visible .mat-focus-indicator::before {
  content: "";
}
.mat-step-header:hover[aria-disabled=true] {
  cursor: default;
}
.mat-step-header:hover:not([aria-disabled]), .mat-step-header:hover[aria-disabled=false] {
  background-color: var(--mat-stepper-header-hover-state-layer-color, color-mix(in srgb, var(--mat-sys-on-surface) calc(var(--mat-sys-hover-state-layer-opacity) * 100%), transparent));
  border-radius: var(--mat-stepper-header-hover-state-layer-shape, var(--mat-sys-corner-medium));
}
.mat-step-header.cdk-keyboard-focused, .mat-step-header.cdk-program-focused {
  background-color: var(--mat-stepper-header-focus-state-layer-color, color-mix(in srgb, var(--mat-sys-on-surface) calc(var(--mat-sys-focus-state-layer-opacity) * 100%), transparent));
  border-radius: var(--mat-stepper-header-focus-state-layer-shape, var(--mat-sys-corner-medium));
}
@media (hover: none) {
  .mat-step-header:hover {
    background: none;
  }
}
@media (forced-colors: active) {
  .mat-step-header {
    outline: solid 1px;
  }
  .mat-step-header[aria-selected=true] .mat-step-label {
    text-decoration: underline;
  }
  .mat-step-header[aria-disabled=true] {
    outline-color: GrayText;
  }
  .mat-step-header[aria-disabled=true] .mat-step-label,
  .mat-step-header[aria-disabled=true] .mat-step-icon,
  .mat-step-header[aria-disabled=true] .mat-step-optional {
    color: GrayText;
  }
}

.mat-step-optional {
  font-size: 12px;
  color: var(--mat-stepper-header-optional-label-text-color, var(--mat-sys-on-surface-variant));
}

.mat-step-sub-label-error {
  font-size: 12px;
  font-weight: normal;
}

.mat-step-icon {
  border-radius: 50%;
  height: 24px;
  width: 24px;
  flex-shrink: 0;
  position: relative;
  color: var(--mat-stepper-header-icon-foreground-color, var(--mat-sys-surface));
  background-color: var(--mat-stepper-header-icon-background-color, var(--mat-sys-on-surface-variant));
}

.mat-step-icon-content {
  position: absolute;
  top: 50%;
  left: 50%;
  transform: translate(-50%, -50%);
  display: flex;
}

.mat-step-icon .mat-icon {
  font-size: 16px;
  height: 16px;
  width: 16px;
}

.mat-step-icon-state-error {
  background-color: var(--mat-stepper-header-error-state-icon-background-color, transparent);
  color: var(--mat-stepper-header-error-state-icon-foreground-color, var(--mat-sys-error));
}
.mat-step-icon-state-error .mat-icon {
  font-size: 24px;
  height: 24px;
  width: 24px;
}

.mat-step-label {
  display: inline-block;
  white-space: nowrap;
  overflow: hidden;
  text-overflow: ellipsis;
  min-width: 50px;
  vertical-align: middle;
  font-family: var(--mat-stepper-header-label-text-font, var(--mat-sys-title-small-font));
  font-size: var(--mat-stepper-header-label-text-size, var(--mat-sys-title-small-size));
  font-weight: var(--mat-stepper-header-label-text-weight, var(--mat-sys-title-small-weight));
  color: var(--mat-stepper-header-label-text-color, var(--mat-sys-on-surface-variant));
}
.mat-step-label.mat-step-label-active {
  color: var(--mat-stepper-header-selected-state-label-text-color, var(--mat-sys-on-surface-variant));
}
.mat-step-label.mat-step-label-error {
  color: var(--mat-stepper-header-error-state-label-text-color, var(--mat-sys-error));
  font-size: var(--mat-stepper-header-error-state-label-text-size, var(--mat-sys-title-small-size));
}
.mat-step-label.mat-step-label-selected {
  font-size: var(--mat-stepper-header-selected-state-label-text-size, var(--mat-sys-title-small-size));
  font-weight: var(--mat-stepper-header-selected-state-label-text-weight, var(--mat-sys-title-small-weight));
}
.mat-step-header-empty-label .mat-step-label {
  min-width: 0;
}

.mat-step-text-label {
  text-overflow: ellipsis;
  overflow: hidden;
}

.mat-step-header .mat-step-header-ripple {
  top: 0;
  left: 0;
  right: 0;
  bottom: 0;
  position: absolute;
  pointer-events: none;
}

.mat-step-icon-selected {
  background-color: var(--mat-stepper-header-selected-state-icon-background-color, var(--mat-sys-primary));
  color: var(--mat-stepper-header-selected-state-icon-foreground-color, var(--mat-sys-on-primary));
}

.mat-step-icon-state-done {
  background-color: var(--mat-stepper-header-done-state-icon-background-color, var(--mat-sys-primary));
  color: var(--mat-stepper-header-done-state-icon-foreground-color, var(--mat-sys-on-primary));
}

.mat-step-icon-state-edit {
  background-color: var(--mat-stepper-header-edit-state-icon-background-color, var(--mat-sys-primary));
  color: var(--mat-stepper-header-edit-state-icon-foreground-color, var(--mat-sys-on-primary));
}
`],encapsulation:2,changeDetection:0})}return t})(),NV=(()=>{class t{templateRef=d(vt);name;constructor(){}static \u0275fac=function(i){return new(i||t)};static \u0275dir=A({type:t,selectors:[["ng-template","matStepperIcon",""]],inputs:{name:[0,"matStepperIcon","name"]}})}return t})(),PV=(()=>{class t{_template=d(vt);constructor(){}static \u0275fac=function(i){return new(i||t)};static \u0275dir=A({type:t,selectors:[["ng-template","matStepContent",""]]})}return t})(),yy=(()=>{class t extends gh{_errorStateMatcher=d(go,{skipSelf:!0});_viewContainerRef=d(Vt);_isSelected=ge.EMPTY;stepLabel=void 0;color;_lazyContent;_portal;ngAfterContentInit(){this._isSelected=this._stepper.steps.changes.pipe(gt(()=>this._stepper.selectionChange.pipe(Q(e=>e.selectedStep===this),et(this._stepper.selected===this)))).subscribe(e=>{e&&this._lazyContent&&!this._portal&&(this._portal=new Un(this._lazyContent._template,this._viewContainerRef))})}ngOnDestroy(){this._isSelected.unsubscribe()}isErrorState(e,i){let r=this._errorStateMatcher.isErrorState(e,i),o=!!(e&&e.invalid&&this.interacted);return r||o}static \u0275fac=(()=>{let e;return function(r){return(e||(e=ct(t)))(r||t)}})();static \u0275cmp=E({type:t,selectors:[["mat-step"]],contentQueries:function(i,r,o){if(i&1&&yt(o,vy,5)(o,PV,5),i&2){let s;$(s=G())&&(r.stepLabel=s.first),$(s=G())&&(r._lazyContent=s.first)}},hostAttrs:["hidden",""],inputs:{color:"color"},exportAs:["matStep"],features:[Ee([{provide:go,useExisting:t},{provide:gh,useExisting:t}]),fe],ngContentSelectors:lk,decls:1,vars:0,consts:[[3,"cdkPortalOutlet"]],template:function(i,r){i&1&&(Ce(),$e(0,bV,2,1,"ng-template"))},dependencies:[Ki],encapsulation:2,changeDetection:0})}return t})(),xy=(()=>{class t extends Qa{_ngZone=d(H);_renderer=d(xe);_animationsDisabled=ke();_cleanupTransition;_isAnimating=M(!1);_stepHeader=void 0;_animatedContainers;_steps=void 0;steps=new En;_icons;animationDone=new U;disableRipple=!1;color;labelPosition="end";headerPosition="top";ariaLabel=null;headerPrefix=sa(null);_iconOverrides={};get animationDuration(){return this._animationDuration}set animationDuration(e){this._animationDuration=/^\d+$/.test(e)?e+"ms":e}_animationDuration="";_isServer=!d(ye).isBrowser;constructor(){super();let i=d(L).nativeElement.nodeName.toLowerCase();this.orientation=i==="mat-vertical-stepper"?"vertical":"horizontal"}ngAfterContentInit(){super.ngAfterContentInit(),this._icons.forEach(({name:e,templateRef:i})=>this._iconOverrides[e]=i),this.steps.changes.pipe(me(this._destroyed)).subscribe(()=>this._stateChanged()),this.selectedIndexChange.pipe(me(this._destroyed)).subscribe(()=>{let e=this._getAnimationDuration();e==="0ms"||e==="0s"?this._onAnimationDone():this._isAnimating.set(!0)}),this._ngZone.runOutsideAngular(()=>{this._animationsDisabled||setTimeout(()=>{this._elementRef.nativeElement.classList.add("mat-stepper-animations-enabled"),this._cleanupTransition=this._renderer.listen(this._elementRef.nativeElement,"transitionend",this._handleTransitionend)},200)})}ngAfterViewInit(){if(super.ngAfterViewInit(),typeof queueMicrotask=="function"){let e=!1;this._animatedContainers.changes.pipe(et(null),me(this._destroyed)).subscribe(()=>queueMicrotask(()=>{e||(e=!0,this.animationDone.emit()),this._stateChanged()}))}}ngOnDestroy(){super.ngOnDestroy(),this._cleanupTransition?.()}_getAnimationDuration(){return this._animationsDisabled?"0ms":this.animationDuration?this.animationDuration:this.orientation==="horizontal"?"500ms":"225ms"}_handleTransitionend=e=>{let i=e.target;if(!i)return;let r=this.orientation==="horizontal"&&e.propertyName==="transform"&&i.classList.contains("mat-horizontal-stepper-content-current"),o=this.orientation==="vertical"&&e.propertyName==="grid-template-rows"&&i.classList.contains("mat-vertical-content-container-active");(r||o)&&this._animatedContainers.find(a=>a.nativeElement===i)&&this._onAnimationDone()};_onAnimationDone(){this._isAnimating.set(!1),this.animationDone.emit()}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=E({type:t,selectors:[["mat-stepper"],["mat-vertical-stepper"],["mat-horizontal-stepper"],["","matStepper",""]],contentQueries:function(i,r,o){if(i&1&&yt(o,yy,5)(o,NV,5),i&2){let s;$(s=G())&&(r._steps=s),$(s=G())&&(r._icons=s)}},viewQuery:function(i,r){if(i&1&&Ve(by,5)(yV,5),i&2){let o;$(o=G())&&(r._stepHeader=o),$(o=G())&&(r._animatedContainers=o)}},hostVars:14,hostBindings:function(i,r){i&2&&(In("--mat-stepper-animation-duration",r._getAnimationDuration()),z("mat-stepper-horizontal",r.orientation==="horizontal")("mat-stepper-vertical",r.orientation==="vertical")("mat-stepper-label-position-end",r.orientation==="horizontal"&&r.labelPosition=="end")("mat-stepper-label-position-bottom",r.orientation==="horizontal"&&r.labelPosition=="bottom")("mat-stepper-header-position-bottom",r.headerPosition==="bottom")("mat-stepper-animating",r._isAnimating()))},inputs:{disableRipple:"disableRipple",color:"color",labelPosition:"labelPosition",headerPosition:"headerPosition",ariaLabel:[0,"aria-label","ariaLabel"],headerPrefix:[1,"headerPrefix"],animationDuration:"animationDuration"},outputs:{animationDone:"animationDone"},exportAs:["matStepper","matVerticalStepper","matHorizontalStepper"],features:[Ee([{provide:Qa,useExisting:t}]),fe],ngContentSelectors:lk,decls:7,vars:2,consts:[["stepTemplate",""],["horizontalStepsTemplate",""],["animatedContainer",""],[1,"mat-horizontal-stepper-wrapper"],[1,"mat-vertical-stepper-wrapper"],[1,"mat-horizontal-stepper-header-wrapper"],[3,"ngTemplateOutlet","ngTemplateOutletContext"],[1,"mat-horizontal-content-container"],["role","tabpanel",1,"mat-horizontal-stepper-content",3,"id","class"],[3,"ngTemplateOutlet"],["role","tabpanel",1,"mat-horizontal-stepper-content",3,"id"],[1,"mat-step"],[1,"mat-vertical-content-container"],["role","region",1,"mat-vertical-stepper-content",3,"id"],[1,"mat-vertical-content"],[3,"click","keydown","tabIndex","id","index","state","label","selected","active","optional","errorMessage","iconOverrides","disableRipple","color"],["aria-orientation","horizontal","role","tablist",1,"mat-horizontal-stepper-header-container"],[1,"mat-stepper-horizontal-line"]],template:function(i,r){if(i&1&&(Ce(),N(0,xV,1,0),N(1,EV,6,1,"div",3)(2,kV,4,1,"div",4),$e(3,MV,1,27,"ng-template",null,0,Xn)(5,RV,3,1,"ng-template",null,1,Xn)),i&2){let o;P(r._isServer?0:-1),p(),P((o=r.orientation)==="horizontal"?1:o==="vertical"?2:-1)}},dependencies:[qi,by],styles:[`.mat-stepper-vertical,
.mat-stepper-horizontal {
  display: block;
  font-family: var(--mat-stepper-container-text-font, var(--mat-sys-body-medium-font));
  background: var(--mat-stepper-container-color, var(--mat-sys-surface));
}

.mat-horizontal-stepper-header-wrapper {
  align-items: center;
  display: flex;
}

.mat-horizontal-stepper-header-container {
  white-space: nowrap;
  display: flex;
  align-items: center;
  flex-grow: 1;
}
.mat-stepper-label-position-bottom .mat-horizontal-stepper-header-container {
  align-items: flex-start;
}
.mat-stepper-header-position-bottom .mat-horizontal-stepper-header-container {
  order: 1;
}

.mat-stepper-horizontal-line {
  border-top-width: 1px;
  border-top-style: solid;
  flex: auto;
  height: 0;
  margin: 0 -16px;
  min-width: 32px;
  border-top-color: var(--mat-stepper-line-color, var(--mat-sys-outline));
}
.mat-stepper-label-position-bottom .mat-stepper-horizontal-line {
  margin: 0;
  min-width: 0;
  position: relative;
  top: calc(calc((var(--mat-stepper-header-height, 72px) - 24px) / 2) + 12px);
}

.mat-stepper-label-position-bottom .mat-horizontal-stepper-header:not(:first-child)::before, [dir=rtl] .mat-stepper-label-position-bottom .mat-horizontal-stepper-header:not(:last-child)::before, .mat-stepper-label-position-bottom .mat-horizontal-stepper-header:not(:last-child)::after, [dir=rtl] .mat-stepper-label-position-bottom .mat-horizontal-stepper-header:not(:first-child)::after {
  border-top-width: 1px;
  border-top-style: solid;
  content: "";
  display: inline-block;
  height: 0;
  position: absolute;
  width: calc(50% - 20px);
}

.mat-horizontal-stepper-header {
  display: flex;
  overflow: hidden;
  align-items: center;
  padding: 0 24px;
  height: var(--mat-stepper-header-height, 72px);
}
.mat-horizontal-stepper-header .mat-step-icon {
  margin-right: 8px;
  flex: none;
}
[dir=rtl] .mat-horizontal-stepper-header .mat-step-icon {
  margin-right: 0;
  margin-left: 8px;
}
.mat-horizontal-stepper-header.mat-step-header-empty-label .mat-step-icon {
  margin: 0;
}
.mat-horizontal-stepper-header::before, .mat-horizontal-stepper-header::after {
  border-top-color: var(--mat-stepper-line-color, var(--mat-sys-outline));
}
.mat-stepper-label-position-bottom .mat-horizontal-stepper-header {
  padding: calc((var(--mat-stepper-header-height, 72px) - 24px) / 2) 24px;
}
.mat-stepper-label-position-bottom .mat-horizontal-stepper-header::before, .mat-stepper-label-position-bottom .mat-horizontal-stepper-header::after {
  top: calc(calc((var(--mat-stepper-header-height, 72px) - 24px) / 2) + 12px);
}
.mat-stepper-label-position-bottom .mat-horizontal-stepper-header {
  box-sizing: border-box;
  flex-direction: column;
  height: auto;
}
.mat-stepper-label-position-bottom .mat-horizontal-stepper-header:not(:last-child)::after, [dir=rtl] .mat-stepper-label-position-bottom .mat-horizontal-stepper-header:not(:first-child)::after {
  right: 0;
}
.mat-stepper-label-position-bottom .mat-horizontal-stepper-header:not(:first-child)::before, [dir=rtl] .mat-stepper-label-position-bottom .mat-horizontal-stepper-header:not(:last-child)::before {
  left: 0;
}
[dir=rtl] .mat-stepper-label-position-bottom .mat-horizontal-stepper-header:last-child::before, [dir=rtl] .mat-stepper-label-position-bottom .mat-horizontal-stepper-header:first-child::after {
  display: none;
}
.mat-stepper-label-position-bottom .mat-horizontal-stepper-header .mat-step-icon {
  margin-right: 0;
  margin-left: 0;
}
.mat-stepper-label-position-bottom .mat-horizontal-stepper-header .mat-step-label {
  padding: 16px 0 0 0;
  text-align: center;
  width: 100%;
}

.mat-vertical-stepper-header {
  display: flex;
  align-items: center;
  height: 24px;
  padding: calc((var(--mat-stepper-header-height, 72px) - 24px) / 2) 24px;
}
.mat-vertical-stepper-header .mat-step-icon {
  margin-right: 12px;
}
[dir=rtl] .mat-vertical-stepper-header .mat-step-icon {
  margin-right: 0;
  margin-left: 12px;
}

.mat-horizontal-stepper-wrapper {
  display: flex;
  flex-direction: column;
}

.mat-horizontal-stepper-content {
  visibility: hidden;
  overflow: hidden;
  outline: 0;
  height: 0;
}
.mat-stepper-animations-enabled .mat-horizontal-stepper-content {
  transition: transform var(--mat-stepper-animation-duration, 0) cubic-bezier(0.35, 0, 0.25, 1);
}
.mat-horizontal-stepper-content.mat-horizontal-stepper-content-previous {
  transform: translate3d(-100%, 0, 0);
}
.mat-horizontal-stepper-content.mat-horizontal-stepper-content-next {
  transform: translate3d(100%, 0, 0);
}
.mat-horizontal-stepper-content.mat-horizontal-stepper-content-current {
  visibility: visible;
  transform: none;
  height: auto;
}
.mat-stepper-horizontal:not(.mat-stepper-animating) .mat-horizontal-stepper-content.mat-horizontal-stepper-content-current {
  overflow: visible;
}

.mat-horizontal-content-container {
  overflow: hidden;
  padding: 0 24px 24px 24px;
}
@media (forced-colors: active) {
  .mat-horizontal-content-container {
    outline: solid 1px;
  }
}
.mat-stepper-header-position-bottom .mat-horizontal-content-container {
  padding: 24px 24px 0 24px;
}

.mat-vertical-content-container {
  display: grid;
  grid-template-rows: 0fr;
  grid-template-columns: 100%;
  margin-left: 36px;
  border: 0;
  position: relative;
}
.mat-stepper-animations-enabled .mat-vertical-content-container {
  transition: grid-template-rows var(--mat-stepper-animation-duration, 0) cubic-bezier(0.4, 0, 0.2, 1);
}
.mat-vertical-content-container.mat-vertical-content-container-active {
  grid-template-rows: 1fr;
}
.mat-step:last-child .mat-vertical-content-container {
  border: none;
}
@media (forced-colors: active) {
  .mat-vertical-content-container {
    outline: solid 1px;
  }
}
[dir=rtl] .mat-vertical-content-container {
  margin-left: 0;
  margin-right: 36px;
}
@supports not (grid-template-rows: 0fr) {
  .mat-vertical-content-container {
    height: 0;
  }
  .mat-vertical-content-container.mat-vertical-content-container-active {
    height: auto;
  }
}

.mat-stepper-vertical-line::before {
  content: "";
  position: absolute;
  left: 0;
  border-left-width: 1px;
  border-left-style: solid;
  border-left-color: var(--mat-stepper-line-color, var(--mat-sys-outline));
  top: calc(8px - calc((var(--mat-stepper-header-height, 72px) - 24px) / 2));
  bottom: calc(8px - calc((var(--mat-stepper-header-height, 72px) - 24px) / 2));
}
[dir=rtl] .mat-stepper-vertical-line::before {
  left: auto;
  right: 0;
}

.mat-vertical-stepper-content {
  overflow: hidden;
  outline: 0;
  visibility: hidden;
}
.mat-stepper-animations-enabled .mat-vertical-stepper-content {
  transition: visibility var(--mat-stepper-animation-duration, 0) linear;
}
.mat-vertical-content-container-active > .mat-vertical-stepper-content {
  visibility: visible;
}

.mat-vertical-content {
  padding: 0 24px 24px 24px;
}
`],encapsulation:2,changeDetection:0})}return t})(),mk=(()=>{class t extends sk{static \u0275fac=(()=>{let e;return function(r){return(e||(e=ct(t)))(r||t)}})();static \u0275dir=A({type:t,selectors:[["button","matStepperNext",""]],hostAttrs:[1,"mat-stepper-next"],hostVars:1,hostBindings:function(i,r){i&2&&mt("type",r.type)},features:[fe]})}return t})(),fk=(()=>{class t extends ak{static \u0275fac=(()=>{let e;return function(r){return(e||(e=ct(t)))(r||t)}})();static \u0275dir=A({type:t,selectors:[["button","matStepperPrevious",""]],hostAttrs:[1,"mat-stepper-previous"],hostVars:1,hostBindings:function(i,r){i&2&&mt("type",r.type)},features:[fe]})}return t})(),hk=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=ae({type:t});static \u0275inj=oe({providers:[go],imports:[Aa,ck,LS,kf,xy,by,Yt]})}return t})();function bh(t){"@babel/helpers - typeof";return typeof Symbol=="function"&&typeof Symbol.iterator=="symbol"?bh=function(n){return typeof n}:bh=function(n){return n&&typeof Symbol=="function"&&n.constructor===Symbol&&n!==Symbol.prototype?"symbol":typeof n},bh(t)}var _k="dahlia",LV=function(n){return n===3?"v3":n},vk="https://js.stripe.com",jV="".concat(vk,"/").concat(_k,"/stripe.js"),BV=/^https:\/\/js\.stripe\.com\/v3\/?(\?.*)?$/,VV=/^https:\/\/js\.stripe\.com\/(v3|[a-z]+)\/stripe\.js(\?.*)?$/,pk="loadStripe.setLoadParameters was called but an existing Stripe.js script already exists in the document; existing script parameters will be used",HV=function(n){return BV.test(n)||VV.test(n)},UV=function(){for(var n=document.querySelectorAll('script[src^="'.concat(vk,'"]')),e=0;e<n.length;e++){var i=n[e];if(HV(i.src))return i}return null},gk=function(n){var e=n&&!n.advancedFraudSignals?"?advancedFraudSignals=false":"",i=document.createElement("script");i.src="".concat(jV).concat(e);var r=document.head||document.body;if(!r)throw new Error("Expected document.body not to be null. Stripe.js requires a <body> element.");return r.appendChild(i),i},zV=function(n,e){!n||!n._registerWrapper||n._registerWrapper({name:"stripe-js",version:"9.8.0",startTime:e})},hd=null,_h=null,vh=null,$V=function(n){return function(e){n(new Error("Failed to load Stripe.js",{cause:e}))}},GV=function(n,e){return function(){window.Stripe?n(window.Stripe):e(new Error("Stripe.js not available"))}},WV=function(n){return hd!==null?hd:(hd=new Promise(function(e,i){if(typeof window>"u"||typeof document>"u"){e(null);return}if(window.Stripe&&n&&console.warn(pk),window.Stripe){e(window.Stripe);return}try{var r=UV();if(r&&n)console.warn(pk);else if(!r)r=gk(n);else if(r&&vh!==null&&_h!==null){var o;r.removeEventListener("load",vh),r.removeEventListener("error",_h),(o=r.parentNode)===null||o===void 0||o.removeChild(r),r=gk(n)}vh=GV(e,i),_h=$V(i),r.addEventListener("load",vh),r.addEventListener("error",_h)}catch(s){i(s);return}}),hd.catch(function(e){return hd=null,Promise.reject(e)}))},qV=function(n,e,i){if(n===null)return null;var r=e[0];if(typeof r!="string")throw new Error("Expected publishable key to be of type string, got type ".concat(bh(r)," instead."));var o=r.match(/^pk_test/),s=LV(n.version),a=_k;o&&s!==a&&console.warn("Stripe.js@".concat(s," was loaded on the page, but @stripe/stripe-js@").concat("9.8.0"," expected Stripe.js@").concat(a,". This may result in unexpected behavior. For more information, see https://docs.stripe.com/sdks/stripejs-versioning"));var c=n.apply(void 0,e);return zV(c,i),c},pd,bk=!1,yk=function(){return pd||(pd=WV(null).catch(function(n){return pd=null,Promise.reject(n)}),pd)};Promise.resolve().then(function(){return yk()}).catch(function(t){bk||console.warn(t)});var xk=function(){for(var n=arguments.length,e=new Array(n),i=0;i<n;i++)e[i]=arguments[i];bk=!0;var r=Date.now();return yk().then(function(o){return qV(o,e,r)})};var Mr={production:!1,apiUrl:"https://localhost:5001/api/",hubUrl:"https://localhost:5001/hub/notifications",stripePublicKey:"pk_test_51TmWs1I2RxZSngwjKY7RldKQvER13PpTjiEMHwoNpKy6pidWo6fr7to2vOSz3J4SpVlRRjnNnzJOwQC7qGiWxYwW00qFftjgdM"};var zn=class extends Error{constructor(n,e){let i=new.target.prototype;super(`${n}: Status code '${e}'`),this.statusCode=e,this.__proto__=i}},_o=class extends Error{constructor(n="A timeout occurred."){let e=new.target.prototype;super(n),this.__proto__=e}},Zt=class extends Error{constructor(n="An abort occurred."){let e=new.target.prototype;super(n),this.__proto__=e}},yh=class extends Error{constructor(n,e){let i=new.target.prototype;super(n),this.transport=e,this.errorType="UnsupportedTransportError",this.__proto__=i}},xh=class extends Error{constructor(n,e){let i=new.target.prototype;super(n),this.transport=e,this.errorType="DisabledTransportError",this.__proto__=i}},Ch=class extends Error{constructor(n,e){let i=new.target.prototype;super(n),this.transport=e,this.errorType="FailedToStartTransportError",this.__proto__=i}},gd=class extends Error{constructor(n){let e=new.target.prototype;super(n),this.errorType="FailedToNegotiateWithServerError",this.__proto__=e}},wh=class extends Error{constructor(n,e){let i=new.target.prototype;super(n),this.innerErrors=e,this.__proto__=i}};var Xa=class{constructor(n,e,i){this.statusCode=n,this.statusText=e,this.content=i}},er=class{get(n,e){return this.send(Z(b({},e),{method:"GET",url:n}))}post(n,e){return this.send(Z(b({},e),{method:"POST",url:n}))}delete(n,e){return this.send(Z(b({},e),{method:"DELETE",url:n}))}getCookieString(n){return""}};var w=(function(t){return t[t.Trace=0]="Trace",t[t.Debug=1]="Debug",t[t.Information=2]="Information",t[t.Warning=3]="Warning",t[t.Error=4]="Error",t[t.Critical=5]="Critical",t[t.None=6]="None",t})(w||{});var tr=class{constructor(){}log(n,e){}};tr.instance=new tr;var Ck="10.0.0";var Ye=class{static isRequired(n,e){if(n==null)throw new Error(`The '${e}' argument is required.`)}static isNotEmpty(n,e){if(!n||n.match(/^\s*$/))throw new Error(`The '${e}' argument should not be empty.`)}static isIn(n,e,i){if(!(n in e))throw new Error(`Unknown ${i} value: ${n}.`)}},Ke=class t{static get isBrowser(){return!t.isNode&&typeof window=="object"&&typeof window.document=="object"}static get isWebWorker(){return!t.isNode&&typeof self=="object"&&"importScripts"in self}static get isReactNative(){return!t.isNode&&typeof window=="object"&&typeof window.document>"u"}static get isNode(){return typeof process<"u"&&process.release&&process.release.name==="node"}};function vo(t,n){let e="";return Ii(t)?(e=`Binary data of length ${t.byteLength}`,n&&(e+=`. Content: '${YV(t)}'`)):typeof t=="string"&&(e=`String data of length ${t.length}`,n&&(e+=`. Content: '${t}'`)),e}function YV(t){let n=new Uint8Array(t),e="";return n.forEach(i=>{let r=i<16?"0":"";e+=`0x${r}${i.toString(16)} `}),e.substring(0,e.length-1)}function Ii(t){return t&&typeof ArrayBuffer<"u"&&(t instanceof ArrayBuffer||t.constructor&&t.constructor.name==="ArrayBuffer")}async function Eh(t,n,e,i,r,o){let s={},[a,c]=nr();s[a]=c,t.log(w.Trace,`(${n} transport) sending data. ${vo(r,o.logMessageContent)}.`);let l=Ii(r)?"arraybuffer":"text",u=await e.post(i,{content:r,headers:b(b({},s),o.headers),responseType:l,timeout:o.timeout,withCredentials:o.withCredentials});t.log(w.Trace,`(${n} transport) request complete. Response status: ${u.statusCode}.`)}function wk(t){return t===void 0?new Ds(w.Information):t===null?tr.instance:t.log!==void 0?t:new Ds(t)}var Dh=class{constructor(n,e){this._subject=n,this._observer=e}dispose(){let n=this._subject.observers.indexOf(this._observer);n>-1&&this._subject.observers.splice(n,1),this._subject.observers.length===0&&this._subject.cancelCallback&&this._subject.cancelCallback().catch(e=>{})}},Ds=class{constructor(n){this._minLevel=n,this.out=console}log(n,e){if(n>=this._minLevel){let i=`[${new Date().toISOString()}] ${w[n]}: ${e}`;switch(n){case w.Critical:case w.Error:this.out.error(i);break;case w.Warning:this.out.warn(i);break;case w.Information:this.out.info(i);break;default:this.out.log(i);break}}}};function nr(){let t="X-SignalR-User-Agent";return Ke.isNode&&(t="User-Agent"),[t,ZV(Ck,QV(),KV(),XV())]}function ZV(t,n,e,i){let r="Microsoft SignalR/",o=t.split(".");return r+=`${o[0]}.${o[1]}`,r+=` (${t}; `,n&&n!==""?r+=`${n}; `:r+="Unknown OS; ",r+=`${e}`,i?r+=`; ${i}`:r+="; Unknown Runtime Version",r+=")",r}function QV(){if(Ke.isNode)switch(process.platform){case"win32":return"Windows NT";case"darwin":return"macOS";case"linux":return"Linux";default:return process.platform}else return""}function XV(){if(Ke.isNode)return process.versions.node}function KV(){return Ke.isNode?"NodeJS":"Browser"}function Sh(t){return t.stack?t.stack:t.message?t.message:`${t}`}function Dk(){if(typeof globalThis<"u")return globalThis;if(typeof self<"u")return self;if(typeof window<"u")return window;if(typeof global<"u")return global;throw new Error("could not find global")}var Ih=class extends er{constructor(n){if(super(),this._logger=n,typeof fetch>"u"||Ke.isNode){let e=typeof __webpack_require__=="function"?__non_webpack_require__:Ss;this._jar=new(e("tough-cookie")).CookieJar,typeof fetch>"u"?this._fetchType=e("node-fetch"):this._fetchType=fetch,this._fetchType=e("fetch-cookie")(this._fetchType,this._jar)}else this._fetchType=fetch.bind(Dk());if(typeof AbortController>"u"){let e=typeof __webpack_require__=="function"?__non_webpack_require__:Ss;this._abortControllerType=e("abort-controller")}else this._abortControllerType=AbortController}async send(n){if(n.abortSignal&&n.abortSignal.aborted)throw new Zt;if(!n.method)throw new Error("No method defined.");if(!n.url)throw new Error("No url defined.");let e=new this._abortControllerType,i;n.abortSignal&&(n.abortSignal.onabort=()=>{e.abort(),i=new Zt});let r=null;if(n.timeout){let c=n.timeout;r=setTimeout(()=>{e.abort(),this._logger.log(w.Warning,"Timeout from HTTP request."),i=new _o},c)}n.content===""&&(n.content=void 0),n.content&&(n.headers=n.headers||{},Ii(n.content)?n.headers["Content-Type"]="application/octet-stream":n.headers["Content-Type"]="text/plain;charset=UTF-8");let o;try{o=await this._fetchType(n.url,{body:n.content,cache:"no-cache",credentials:n.withCredentials===!0?"include":"same-origin",headers:b({"X-Requested-With":"XMLHttpRequest"},n.headers),method:n.method,mode:"cors",redirect:"follow",signal:e.signal})}catch(c){throw i||(this._logger.log(w.Warning,`Error from HTTP request. ${c}.`),c)}finally{r&&clearTimeout(r),n.abortSignal&&(n.abortSignal.onabort=null)}if(!o.ok){let c=await Ek(o,"text");throw new zn(c||o.statusText,o.status)}let a=await Ek(o,n.responseType);return new Xa(o.status,o.statusText,a)}getCookieString(n){let e="";return Ke.isNode&&this._jar&&this._jar.getCookies(n,(i,r)=>e=r.join("; ")),e}};function Ek(t,n){let e;switch(n){case"arraybuffer":e=t.arrayBuffer();break;case"text":e=t.text();break;case"blob":case"document":case"json":throw new Error(`${n} is not supported.`);default:e=t.text();break}return e}var kh=class extends er{constructor(n){super(),this._logger=n}send(n){return n.abortSignal&&n.abortSignal.aborted?Promise.reject(new Zt):n.method?n.url?new Promise((e,i)=>{let r=new XMLHttpRequest;r.open(n.method,n.url,!0),r.withCredentials=n.withCredentials===void 0?!0:n.withCredentials,r.setRequestHeader("X-Requested-With","XMLHttpRequest"),n.content===""&&(n.content=void 0),n.content&&(Ii(n.content)?r.setRequestHeader("Content-Type","application/octet-stream"):r.setRequestHeader("Content-Type","text/plain;charset=UTF-8"));let o=n.headers;o&&Object.keys(o).forEach(s=>{r.setRequestHeader(s,o[s])}),n.responseType&&(r.responseType=n.responseType),n.abortSignal&&(n.abortSignal.onabort=()=>{r.abort(),i(new Zt)}),n.timeout&&(r.timeout=n.timeout),r.onload=()=>{n.abortSignal&&(n.abortSignal.onabort=null),r.status>=200&&r.status<300?e(new Xa(r.status,r.statusText,r.response||r.responseText)):i(new zn(r.response||r.responseText||r.statusText,r.status))},r.onerror=()=>{this._logger.log(w.Warning,`Error from HTTP request. ${r.status}: ${r.statusText}.`),i(new zn(r.statusText,r.status))},r.ontimeout=()=>{this._logger.log(w.Warning,"Timeout from HTTP request."),i(new _o)},r.send(n.content)}):Promise.reject(new Error("No url defined.")):Promise.reject(new Error("No method defined."))}};var Mh=class extends er{constructor(n){if(super(),typeof fetch<"u"||Ke.isNode)this._httpClient=new Ih(n);else if(typeof XMLHttpRequest<"u")this._httpClient=new kh(n);else throw new Error("No usable HttpClient found.")}send(n){return n.abortSignal&&n.abortSignal.aborted?Promise.reject(new Zt):n.method?n.url?this._httpClient.send(n):Promise.reject(new Error("No url defined.")):Promise.reject(new Error("No method defined."))}getCookieString(n){return this._httpClient.getCookieString(n)}};var $n=class t{static write(n){return`${n}${t.RecordSeparator}`}static parse(n){if(n[n.length-1]!==t.RecordSeparator)throw new Error("Message is incomplete.");let e=n.split(t.RecordSeparator);return e.pop(),e}};$n.RecordSeparatorCode=30;$n.RecordSeparator=String.fromCharCode($n.RecordSeparatorCode);var Th=class{writeHandshakeRequest(n){return $n.write(JSON.stringify(n))}parseHandshakeResponse(n){let e,i;if(Ii(n)){let a=new Uint8Array(n),c=a.indexOf($n.RecordSeparatorCode);if(c===-1)throw new Error("Message is incomplete.");let l=c+1;e=String.fromCharCode.apply(null,Array.prototype.slice.call(a.slice(0,l))),i=a.byteLength>l?a.slice(l).buffer:null}else{let a=n,c=a.indexOf($n.RecordSeparator);if(c===-1)throw new Error("Message is incomplete.");let l=c+1;e=a.substring(0,l),i=a.length>l?a.substring(l):null}let r=$n.parse(e),o=JSON.parse(r[0]);if(o.type)throw new Error("Expected a handshake response from the server.");return[i,o]}};var le=(function(t){return t[t.Invocation=1]="Invocation",t[t.StreamItem=2]="StreamItem",t[t.Completion=3]="Completion",t[t.StreamInvocation=4]="StreamInvocation",t[t.CancelInvocation=5]="CancelInvocation",t[t.Ping=6]="Ping",t[t.Close=7]="Close",t[t.Ack=8]="Ack",t[t.Sequence=9]="Sequence",t})(le||{});var Ah=class{constructor(){this.observers=[]}next(n){for(let e of this.observers)e.next(n)}error(n){for(let e of this.observers)e.error&&e.error(n)}complete(){for(let n of this.observers)n.complete&&n.complete()}subscribe(n){return this.observers.push(n),new Dh(this,n)}};var Rh=class{constructor(n,e,i){this._bufferSize=1e5,this._messages=[],this._totalMessageCount=0,this._waitForSequenceMessage=!1,this._nextReceivingSequenceId=1,this._latestReceivedSequenceId=0,this._bufferedByteCount=0,this._reconnectInProgress=!1,this._protocol=n,this._connection=e,this._bufferSize=i}async _send(n){let e=this._protocol.writeMessage(n),i=Promise.resolve();if(this._isInvocationMessage(n)){this._totalMessageCount++;let r=()=>{},o=()=>{};Ii(e)?this._bufferedByteCount+=e.byteLength:this._bufferedByteCount+=e.length,this._bufferedByteCount>=this._bufferSize&&(i=new Promise((s,a)=>{r=s,o=a})),this._messages.push(new Cy(e,this._totalMessageCount,r,o))}try{this._reconnectInProgress||await this._connection.send(e)}catch{this._disconnected()}await i}_ack(n){let e=-1;for(let i=0;i<this._messages.length;i++){let r=this._messages[i];if(r._id<=n.sequenceId)e=i,Ii(r._message)?this._bufferedByteCount-=r._message.byteLength:this._bufferedByteCount-=r._message.length,r._resolver();else if(this._bufferedByteCount<this._bufferSize)r._resolver();else break}e!==-1&&(this._messages=this._messages.slice(e+1))}_shouldProcessMessage(n){if(this._waitForSequenceMessage)return n.type!==le.Sequence?!1:(this._waitForSequenceMessage=!1,!0);if(!this._isInvocationMessage(n))return!0;let e=this._nextReceivingSequenceId;return this._nextReceivingSequenceId++,e<=this._latestReceivedSequenceId?(e===this._latestReceivedSequenceId&&this._ackTimer(),!1):(this._latestReceivedSequenceId=e,this._ackTimer(),!0)}_resetSequence(n){if(n.sequenceId>this._nextReceivingSequenceId){this._connection.stop(new Error("Sequence ID greater than amount of messages we've received."));return}this._nextReceivingSequenceId=n.sequenceId}_disconnected(){this._reconnectInProgress=!0,this._waitForSequenceMessage=!0}async _resend(){let n=this._messages.length!==0?this._messages[0]._id:this._totalMessageCount+1;await this._connection.send(this._protocol.writeMessage({type:le.Sequence,sequenceId:n}));let e=this._messages;for(let i of e)await this._connection.send(i._message);this._reconnectInProgress=!1}_dispose(n){n??(n=new Error("Unable to reconnect to server."));for(let e of this._messages)e._rejector(n)}_isInvocationMessage(n){switch(n.type){case le.Invocation:case le.StreamItem:case le.Completion:case le.StreamInvocation:case le.CancelInvocation:return!0;case le.Close:case le.Sequence:case le.Ping:case le.Ack:return!1}}_ackTimer(){this._ackTimerHandle===void 0&&(this._ackTimerHandle=setTimeout(async()=>{try{this._reconnectInProgress||await this._connection.send(this._protocol.writeMessage({type:le.Ack,sequenceId:this._latestReceivedSequenceId}))}catch{}clearTimeout(this._ackTimerHandle),this._ackTimerHandle=void 0},1e3))}},Cy=class{constructor(n,e,i,r){this._message=n,this._id=e,this._resolver=i,this._rejector=r}};var JV=30*1e3,eH=15*1e3,tH=1e5,Je=(function(t){return t.Disconnected="Disconnected",t.Connecting="Connecting",t.Connected="Connected",t.Disconnecting="Disconnecting",t.Reconnecting="Reconnecting",t})(Je||{}),_d=class t{static create(n,e,i,r,o,s,a){return new t(n,e,i,r,o,s,a)}constructor(n,e,i,r,o,s,a){this._nextKeepAlive=0,this._freezeEventListener=()=>{this._logger.log(w.Warning,"The page is being frozen, this will likely lead to the connection being closed and messages being lost. For more information see the docs at https://learn.microsoft.com/aspnet/core/signalr/javascript-client#bsleep")},Ye.isRequired(n,"connection"),Ye.isRequired(e,"logger"),Ye.isRequired(i,"protocol"),this.serverTimeoutInMilliseconds=o??JV,this.keepAliveIntervalInMilliseconds=s??eH,this._statefulReconnectBufferSize=a??tH,this._logger=e,this._protocol=i,this.connection=n,this._reconnectPolicy=r,this._handshakeProtocol=new Th,this.connection.onreceive=c=>this._processIncomingData(c),this.connection.onclose=c=>this._connectionClosed(c),this._callbacks={},this._methods={},this._closedCallbacks=[],this._reconnectingCallbacks=[],this._reconnectedCallbacks=[],this._invocationId=0,this._receivedHandshakeResponse=!1,this._connectionState=Je.Disconnected,this._connectionStarted=!1,this._cachedPingMessage=this._protocol.writeMessage({type:le.Ping})}get state(){return this._connectionState}get connectionId(){return this.connection&&this.connection.connectionId||null}get baseUrl(){return this.connection.baseUrl||""}set baseUrl(n){if(this._connectionState!==Je.Disconnected&&this._connectionState!==Je.Reconnecting)throw new Error("The HubConnection must be in the Disconnected or Reconnecting state to change the url.");if(!n)throw new Error("The HubConnection url must be a valid url.");this.connection.baseUrl=n}start(){return this._startPromise=this._startWithStateTransitions(),this._startPromise}async _startWithStateTransitions(){if(this._connectionState!==Je.Disconnected)return Promise.reject(new Error("Cannot start a HubConnection that is not in the 'Disconnected' state."));this._connectionState=Je.Connecting,this._logger.log(w.Debug,"Starting HubConnection.");try{await this._startInternal(),Ke.isBrowser&&window.document.addEventListener("freeze",this._freezeEventListener),this._connectionState=Je.Connected,this._connectionStarted=!0,this._logger.log(w.Debug,"HubConnection connected successfully.")}catch(n){return this._connectionState=Je.Disconnected,this._logger.log(w.Debug,`HubConnection failed to start successfully because of error '${n}'.`),Promise.reject(n)}}async _startInternal(){this._stopDuringStartError=void 0,this._receivedHandshakeResponse=!1;let n=new Promise((e,i)=>{this._handshakeResolver=e,this._handshakeRejecter=i});await this.connection.start(this._protocol.transferFormat);try{let e=this._protocol.version;this.connection.features.reconnect||(e=1);let i={protocol:this._protocol.name,version:e};if(this._logger.log(w.Debug,"Sending handshake request."),await this._sendMessage(this._handshakeProtocol.writeHandshakeRequest(i)),this._logger.log(w.Information,`Using HubProtocol '${this._protocol.name}'.`),this._cleanupTimeout(),this._resetTimeoutPeriod(),this._resetKeepAliveInterval(),await n,this._stopDuringStartError)throw this._stopDuringStartError;this.connection.features.reconnect&&(this._messageBuffer=new Rh(this._protocol,this.connection,this._statefulReconnectBufferSize),this.connection.features.disconnected=this._messageBuffer._disconnected.bind(this._messageBuffer),this.connection.features.resend=()=>{if(this._messageBuffer)return this._messageBuffer._resend()}),this.connection.features.inherentKeepAlive||await this._sendMessage(this._cachedPingMessage)}catch(e){throw this._logger.log(w.Debug,`Hub handshake failed with error '${e}' during start(). Stopping HubConnection.`),this._cleanupTimeout(),this._cleanupPingTimer(),await this.connection.stop(e),e}}async stop(){let n=this._startPromise;this.connection.features.reconnect=!1,this._stopPromise=this._stopInternal(),await this._stopPromise;try{await n}catch{}}_stopInternal(n){if(this._connectionState===Je.Disconnected)return this._logger.log(w.Debug,`Call to HubConnection.stop(${n}) ignored because it is already in the disconnected state.`),Promise.resolve();if(this._connectionState===Je.Disconnecting)return this._logger.log(w.Debug,`Call to HttpConnection.stop(${n}) ignored because the connection is already in the disconnecting state.`),this._stopPromise;let e=this._connectionState;return this._connectionState=Je.Disconnecting,this._logger.log(w.Debug,"Stopping HubConnection."),this._reconnectDelayHandle?(this._logger.log(w.Debug,"Connection stopped during reconnect delay. Done reconnecting."),clearTimeout(this._reconnectDelayHandle),this._reconnectDelayHandle=void 0,this._completeClose(),Promise.resolve()):(e===Je.Connected&&this._sendCloseMessage(),this._cleanupTimeout(),this._cleanupPingTimer(),this._stopDuringStartError=n||new Zt("The connection was stopped before the hub handshake could complete."),this.connection.stop(n))}async _sendCloseMessage(){try{await this._sendWithProtocol(this._createCloseMessage())}catch{}}stream(n,...e){let[i,r]=this._replaceStreamingParams(e),o=this._createStreamInvocation(n,e,r),s,a=new Ah;return a.cancelCallback=()=>{let c=this._createCancelInvocation(o.invocationId);return delete this._callbacks[o.invocationId],s.then(()=>this._sendWithProtocol(c))},this._callbacks[o.invocationId]=(c,l)=>{if(l){a.error(l);return}else c&&(c.type===le.Completion?c.error?a.error(new Error(c.error)):a.complete():a.next(c.item))},s=this._sendWithProtocol(o).catch(c=>{a.error(c),delete this._callbacks[o.invocationId]}),this._launchStreams(i,s),a}_sendMessage(n){return this._resetKeepAliveInterval(),this.connection.send(n)}_sendWithProtocol(n){return this._messageBuffer?this._messageBuffer._send(n):this._sendMessage(this._protocol.writeMessage(n))}send(n,...e){let[i,r]=this._replaceStreamingParams(e),o=this._sendWithProtocol(this._createInvocation(n,e,!0,r));return this._launchStreams(i,o),o}invoke(n,...e){let[i,r]=this._replaceStreamingParams(e),o=this._createInvocation(n,e,!1,r);return new Promise((a,c)=>{this._callbacks[o.invocationId]=(u,m)=>{if(m){c(m);return}else u&&(u.type===le.Completion?u.error?c(new Error(u.error)):a(u.result):c(new Error(`Unexpected message type: ${u.type}`)))};let l=this._sendWithProtocol(o).catch(u=>{c(u),delete this._callbacks[o.invocationId]});this._launchStreams(i,l)})}on(n,e){!n||!e||(n=n.toLowerCase(),this._methods[n]||(this._methods[n]=[]),this._methods[n].indexOf(e)===-1&&this._methods[n].push(e))}off(n,e){if(!n)return;n=n.toLowerCase();let i=this._methods[n];if(i)if(e){let r=i.indexOf(e);r!==-1&&(i.splice(r,1),i.length===0&&delete this._methods[n])}else delete this._methods[n]}onclose(n){n&&this._closedCallbacks.push(n)}onreconnecting(n){n&&this._reconnectingCallbacks.push(n)}onreconnected(n){n&&this._reconnectedCallbacks.push(n)}_processIncomingData(n){if(this._cleanupTimeout(),this._receivedHandshakeResponse||(n=this._processHandshakeResponse(n),this._receivedHandshakeResponse=!0),n){let e=this._protocol.parseMessages(n,this._logger);for(let i of e)if(!(this._messageBuffer&&!this._messageBuffer._shouldProcessMessage(i)))switch(i.type){case le.Invocation:this._invokeClientMethod(i).catch(r=>{this._logger.log(w.Error,`Invoke client method threw error: ${Sh(r)}`)});break;case le.StreamItem:case le.Completion:{let r=this._callbacks[i.invocationId];if(r){i.type===le.Completion&&delete this._callbacks[i.invocationId];try{r(i)}catch(o){this._logger.log(w.Error,`Stream callback threw error: ${Sh(o)}`)}}break}case le.Ping:break;case le.Close:{this._logger.log(w.Information,"Close message received from server.");let r=i.error?new Error("Server returned an error on close: "+i.error):void 0;i.allowReconnect===!0?this.connection.stop(r):this._stopPromise=this._stopInternal(r);break}case le.Ack:this._messageBuffer&&this._messageBuffer._ack(i);break;case le.Sequence:this._messageBuffer&&this._messageBuffer._resetSequence(i);break;default:this._logger.log(w.Warning,`Invalid message type: ${i.type}.`);break}}this._resetTimeoutPeriod()}_processHandshakeResponse(n){let e,i;try{[i,e]=this._handshakeProtocol.parseHandshakeResponse(n)}catch(r){let o="Error parsing handshake response: "+r;this._logger.log(w.Error,o);let s=new Error(o);throw this._handshakeRejecter(s),s}if(e.error){let r="Server returned handshake error: "+e.error;this._logger.log(w.Error,r);let o=new Error(r);throw this._handshakeRejecter(o),o}else this._logger.log(w.Debug,"Server handshake complete.");return this._handshakeResolver(),i}_resetKeepAliveInterval(){this.connection.features.inherentKeepAlive||(this._nextKeepAlive=new Date().getTime()+this.keepAliveIntervalInMilliseconds,this._cleanupPingTimer())}_resetTimeoutPeriod(){if(!this.connection.features||!this.connection.features.inherentKeepAlive){this._timeoutHandle=setTimeout(()=>this.serverTimeout(),this.serverTimeoutInMilliseconds);let n=this._nextKeepAlive-new Date().getTime();if(n<0){this._connectionState===Je.Connected&&this._trySendPingMessage();return}this._pingServerHandle===void 0&&(n<0&&(n=0),this._pingServerHandle=setTimeout(async()=>{this._connectionState===Je.Connected&&await this._trySendPingMessage()},n))}}serverTimeout(){this.connection.stop(new Error("Server timeout elapsed without receiving a message from the server."))}async _invokeClientMethod(n){let e=n.target.toLowerCase(),i=this._methods[e];if(!i){this._logger.log(w.Warning,`No client method with the name '${e}' found.`),n.invocationId&&(this._logger.log(w.Warning,`No result given for '${e}' method and invocation ID '${n.invocationId}'.`),await this._sendWithProtocol(this._createCompletionMessage(n.invocationId,"Client didn't provide a result.",null)));return}let r=i.slice(),o=!!n.invocationId,s,a,c;for(let l of r)try{let u=s;s=await l.apply(this,n.arguments),o&&s&&u&&(this._logger.log(w.Error,`Multiple results provided for '${e}'. Sending error to server.`),c=this._createCompletionMessage(n.invocationId,"Client provided multiple results.",null)),a=void 0}catch(u){a=u,this._logger.log(w.Error,`A callback for the method '${e}' threw error '${u}'.`)}c?await this._sendWithProtocol(c):o?(a?c=this._createCompletionMessage(n.invocationId,`${a}`,null):s!==void 0?c=this._createCompletionMessage(n.invocationId,null,s):(this._logger.log(w.Warning,`No result given for '${e}' method and invocation ID '${n.invocationId}'.`),c=this._createCompletionMessage(n.invocationId,"Client didn't provide a result.",null)),await this._sendWithProtocol(c)):s&&this._logger.log(w.Error,`Result given for '${e}' method but server is not expecting a result.`)}_connectionClosed(n){this._logger.log(w.Debug,`HubConnection.connectionClosed(${n}) called while in state ${this._connectionState}.`),this._stopDuringStartError=this._stopDuringStartError||n||new Zt("The underlying connection was closed before the hub handshake could complete."),this._handshakeResolver&&this._handshakeResolver(),this._cancelCallbacksWithError(n||new Error("Invocation canceled due to the underlying connection being closed.")),this._cleanupTimeout(),this._cleanupPingTimer(),this._connectionState===Je.Disconnecting?this._completeClose(n):this._connectionState===Je.Connected&&this._reconnectPolicy?this._reconnect(n):this._connectionState===Je.Connected&&this._completeClose(n)}_completeClose(n){if(this._connectionStarted){this._connectionState=Je.Disconnected,this._connectionStarted=!1,this._messageBuffer&&(this._messageBuffer._dispose(n??new Error("Connection closed.")),this._messageBuffer=void 0),Ke.isBrowser&&window.document.removeEventListener("freeze",this._freezeEventListener);try{this._closedCallbacks.forEach(e=>e.apply(this,[n]))}catch(e){this._logger.log(w.Error,`An onclose callback called with error '${n}' threw error '${e}'.`)}}}async _reconnect(n){let e=Date.now(),i=0,r=n!==void 0?n:new Error("Attempting to reconnect due to a unknown error."),o=this._getNextRetryDelay(i,0,r);if(o===null){this._logger.log(w.Debug,"Connection not reconnecting because the IRetryPolicy returned null on the first reconnect attempt."),this._completeClose(n);return}if(this._connectionState=Je.Reconnecting,n?this._logger.log(w.Information,`Connection reconnecting because of error '${n}'.`):this._logger.log(w.Information,"Connection reconnecting."),this._reconnectingCallbacks.length!==0){try{this._reconnectingCallbacks.forEach(s=>s.apply(this,[n]))}catch(s){this._logger.log(w.Error,`An onreconnecting callback called with error '${n}' threw error '${s}'.`)}if(this._connectionState!==Je.Reconnecting){this._logger.log(w.Debug,"Connection left the reconnecting state in onreconnecting callback. Done reconnecting.");return}}for(;o!==null;){if(this._logger.log(w.Information,`Reconnect attempt number ${i+1} will start in ${o} ms.`),await new Promise(s=>{this._reconnectDelayHandle=setTimeout(s,o)}),this._reconnectDelayHandle=void 0,this._connectionState!==Je.Reconnecting){this._logger.log(w.Debug,"Connection left the reconnecting state during reconnect delay. Done reconnecting.");return}try{if(await this._startInternal(),this._connectionState=Je.Connected,this._logger.log(w.Information,"HubConnection reconnected successfully."),this._reconnectedCallbacks.length!==0)try{this._reconnectedCallbacks.forEach(s=>s.apply(this,[this.connection.connectionId]))}catch(s){this._logger.log(w.Error,`An onreconnected callback called with connectionId '${this.connection.connectionId}; threw error '${s}'.`)}return}catch(s){if(this._logger.log(w.Information,`Reconnect attempt failed because of error '${s}'.`),this._connectionState!==Je.Reconnecting){this._logger.log(w.Debug,`Connection moved to the '${this._connectionState}' from the reconnecting state during reconnect attempt. Done reconnecting.`),this._connectionState===Je.Disconnecting&&this._completeClose();return}i++,r=s instanceof Error?s:new Error(s.toString()),o=this._getNextRetryDelay(i,Date.now()-e,r)}}this._logger.log(w.Information,`Reconnect retries have been exhausted after ${Date.now()-e} ms and ${i} failed attempts. Connection disconnecting.`),this._completeClose()}_getNextRetryDelay(n,e,i){try{return this._reconnectPolicy.nextRetryDelayInMilliseconds({elapsedMilliseconds:e,previousRetryCount:n,retryReason:i})}catch(r){return this._logger.log(w.Error,`IRetryPolicy.nextRetryDelayInMilliseconds(${n}, ${e}) threw error '${r}'.`),null}}_cancelCallbacksWithError(n){let e=this._callbacks;this._callbacks={},Object.keys(e).forEach(i=>{let r=e[i];try{r(null,n)}catch(o){this._logger.log(w.Error,`Stream 'error' callback called with '${n}' threw error: ${Sh(o)}`)}})}_cleanupPingTimer(){this._pingServerHandle&&(clearTimeout(this._pingServerHandle),this._pingServerHandle=void 0)}_cleanupTimeout(){this._timeoutHandle&&clearTimeout(this._timeoutHandle)}_createInvocation(n,e,i,r){if(i)return r.length!==0?{target:n,arguments:e,streamIds:r,type:le.Invocation}:{target:n,arguments:e,type:le.Invocation};{let o=this._invocationId;return this._invocationId++,r.length!==0?{target:n,arguments:e,invocationId:o.toString(),streamIds:r,type:le.Invocation}:{target:n,arguments:e,invocationId:o.toString(),type:le.Invocation}}}_launchStreams(n,e){if(n.length!==0){e||(e=Promise.resolve());for(let i in n)n[i].subscribe({complete:()=>{e=e.then(()=>this._sendWithProtocol(this._createCompletionMessage(i)))},error:r=>{let o;r instanceof Error?o=r.message:r&&r.toString?o=r.toString():o="Unknown error",e=e.then(()=>this._sendWithProtocol(this._createCompletionMessage(i,o)))},next:r=>{e=e.then(()=>this._sendWithProtocol(this._createStreamItemMessage(i,r)))}})}}_replaceStreamingParams(n){let e=[],i=[];for(let r=0;r<n.length;r++){let o=n[r];if(this._isObservable(o)){let s=this._invocationId;this._invocationId++,e[s]=o,i.push(s.toString()),n.splice(r,1)}}return[e,i]}_isObservable(n){return n&&n.subscribe&&typeof n.subscribe=="function"}_createStreamInvocation(n,e,i){let r=this._invocationId;return this._invocationId++,i.length!==0?{target:n,arguments:e,invocationId:r.toString(),streamIds:i,type:le.StreamInvocation}:{target:n,arguments:e,invocationId:r.toString(),type:le.StreamInvocation}}_createCancelInvocation(n){return{invocationId:n,type:le.CancelInvocation}}_createStreamItemMessage(n,e){return{invocationId:n,item:e,type:le.StreamItem}}_createCompletionMessage(n,e,i){return e?{error:e,invocationId:n,type:le.Completion}:{invocationId:n,result:i,type:le.Completion}}_createCloseMessage(){return{type:le.Close}}async _trySendPingMessage(){try{await this._sendMessage(this._cachedPingMessage)}catch{this._cleanupPingTimer()}}};var nH=[0,2e3,1e4,3e4,null],vd=class{constructor(n){this._retryDelays=n!==void 0?[...n,null]:nH}nextRetryDelayInMilliseconds(n){return this._retryDelays[n.previousRetryCount]}};var Es=(()=>{class t{}return t.Authorization="Authorization",t.Cookie="Cookie",t})();var Oh=class extends er{constructor(n,e){super(),this._innerClient=n,this._accessTokenFactory=e}async send(n){let e=!0;this._accessTokenFactory&&(!this._accessToken||n.url&&n.url.indexOf("/negotiate?")>0)&&(e=!1,this._accessToken=await this._accessTokenFactory()),this._setAuthorizationHeader(n);let i=await this._innerClient.send(n);return e&&i.statusCode===401&&this._accessTokenFactory?(this._accessToken=await this._accessTokenFactory(),this._setAuthorizationHeader(n),await this._innerClient.send(n)):i}_setAuthorizationHeader(n){n.headers||(n.headers={}),this._accessToken?n.headers[Es.Authorization]=`Bearer ${this._accessToken}`:this._accessTokenFactory&&n.headers[Es.Authorization]&&delete n.headers[Es.Authorization]}getCookieString(n){return this._innerClient.getCookieString(n)}};var zt=(function(t){return t[t.None=0]="None",t[t.WebSockets=1]="WebSockets",t[t.ServerSentEvents=2]="ServerSentEvents",t[t.LongPolling=4]="LongPolling",t})(zt||{}),Pt=(function(t){return t[t.Text=1]="Text",t[t.Binary=2]="Binary",t})(Pt||{});var Nh=class{constructor(){this._isAborted=!1,this.onabort=null}abort(){this._isAborted||(this._isAborted=!0,this.onabort&&this.onabort())}get signal(){return this}get aborted(){return this._isAborted}};var bd=class{get pollAborted(){return this._pollAbort.aborted}constructor(n,e,i){this._httpClient=n,this._logger=e,this._pollAbort=new Nh,this._options=i,this._running=!1,this.onreceive=null,this.onclose=null}async connect(n,e){if(Ye.isRequired(n,"url"),Ye.isRequired(e,"transferFormat"),Ye.isIn(e,Pt,"transferFormat"),this._url=n,this._logger.log(w.Trace,"(LongPolling transport) Connecting."),e===Pt.Binary&&typeof XMLHttpRequest<"u"&&typeof new XMLHttpRequest().responseType!="string")throw new Error("Binary protocols over XmlHttpRequest not implementing advanced features are not supported.");let[i,r]=nr(),o=b({[i]:r},this._options.headers),s={abortSignal:this._pollAbort.signal,headers:o,timeout:1e5,withCredentials:this._options.withCredentials};e===Pt.Binary&&(s.responseType="arraybuffer");let a=`${n}&_=${Date.now()}`;this._logger.log(w.Trace,`(LongPolling transport) polling: ${a}.`);let c=await this._httpClient.get(a,s);c.statusCode!==200?(this._logger.log(w.Error,`(LongPolling transport) Unexpected response code: ${c.statusCode}.`),this._closeError=new zn(c.statusText||"",c.statusCode),this._running=!1):this._running=!0,this._receiving=this._poll(this._url,s)}async _poll(n,e){try{for(;this._running;)try{let i=`${n}&_=${Date.now()}`;this._logger.log(w.Trace,`(LongPolling transport) polling: ${i}.`);let r=await this._httpClient.get(i,e);r.statusCode===204?(this._logger.log(w.Information,"(LongPolling transport) Poll terminated by server."),this._running=!1):r.statusCode!==200?(this._logger.log(w.Error,`(LongPolling transport) Unexpected response code: ${r.statusCode}.`),this._closeError=new zn(r.statusText||"",r.statusCode),this._running=!1):r.content?(this._logger.log(w.Trace,`(LongPolling transport) data received. ${vo(r.content,this._options.logMessageContent)}.`),this.onreceive&&this.onreceive(r.content)):this._logger.log(w.Trace,"(LongPolling transport) Poll timed out, reissuing.")}catch(i){this._running?i instanceof _o?this._logger.log(w.Trace,"(LongPolling transport) Poll timed out, reissuing."):(this._closeError=i,this._running=!1):this._logger.log(w.Trace,`(LongPolling transport) Poll errored after shutdown: ${i.message}`)}}finally{this._logger.log(w.Trace,"(LongPolling transport) Polling complete."),this.pollAborted||this._raiseOnClose()}}async send(n){return this._running?Eh(this._logger,"LongPolling",this._httpClient,this._url,n,this._options):Promise.reject(new Error("Cannot send until the transport is connected"))}async stop(){this._logger.log(w.Trace,"(LongPolling transport) Stopping polling."),this._running=!1,this._pollAbort.abort();try{await this._receiving,this._logger.log(w.Trace,`(LongPolling transport) sending DELETE request to ${this._url}.`);let n={},[e,i]=nr();n[e]=i;let r={headers:b(b({},n),this._options.headers),timeout:this._options.timeout,withCredentials:this._options.withCredentials},o;try{await this._httpClient.delete(this._url,r)}catch(s){o=s}o?o instanceof zn&&(o.statusCode===404?this._logger.log(w.Trace,"(LongPolling transport) A 404 response was returned from sending a DELETE request."):this._logger.log(w.Trace,`(LongPolling transport) Error sending a DELETE request: ${o}`)):this._logger.log(w.Trace,"(LongPolling transport) DELETE request accepted.")}finally{this._logger.log(w.Trace,"(LongPolling transport) Stop finished."),this._raiseOnClose()}}_raiseOnClose(){if(this.onclose){let n="(LongPolling transport) Firing onclose event.";this._closeError&&(n+=" Error: "+this._closeError),this._logger.log(w.Trace,n),this.onclose(this._closeError)}}};var Ph=class{constructor(n,e,i,r){this._httpClient=n,this._accessToken=e,this._logger=i,this._options=r,this.onreceive=null,this.onclose=null}async connect(n,e){return Ye.isRequired(n,"url"),Ye.isRequired(e,"transferFormat"),Ye.isIn(e,Pt,"transferFormat"),this._logger.log(w.Trace,"(SSE transport) Connecting."),this._url=n,this._accessToken&&(n+=(n.indexOf("?")<0?"?":"&")+`access_token=${encodeURIComponent(this._accessToken)}`),new Promise((i,r)=>{let o=!1;if(e!==Pt.Text){r(new Error("The Server-Sent Events transport only supports the 'Text' transfer format"));return}let s;if(Ke.isBrowser||Ke.isWebWorker)s=new this._options.EventSource(n,{withCredentials:this._options.withCredentials});else{let a=this._httpClient.getCookieString(n),c={};c.Cookie=a;let[l,u]=nr();c[l]=u,s=new this._options.EventSource(n,{withCredentials:this._options.withCredentials,headers:b(b({},c),this._options.headers)})}try{s.onmessage=a=>{if(this.onreceive)try{this._logger.log(w.Trace,`(SSE transport) data received. ${vo(a.data,this._options.logMessageContent)}.`),this.onreceive(a.data)}catch(c){this._close(c);return}},s.onerror=a=>{o?this._close():r(new Error("EventSource failed to connect. The connection could not be found on the server, either the connection ID is not present on the server, or a proxy is refusing/buffering the connection. If you have multiple servers check that sticky sessions are enabled."))},s.onopen=()=>{this._logger.log(w.Information,`SSE connected to ${this._url}`),this._eventSource=s,o=!0,i()}}catch(a){r(a);return}})}async send(n){return this._eventSource?Eh(this._logger,"SSE",this._httpClient,this._url,n,this._options):Promise.reject(new Error("Cannot send until the transport is connected"))}stop(){return this._close(),Promise.resolve()}_close(n){this._eventSource&&(this._eventSource.close(),this._eventSource=void 0,this.onclose&&this.onclose(n))}};var Fh=class{constructor(n,e,i,r,o,s){this._logger=i,this._accessTokenFactory=e,this._logMessageContent=r,this._webSocketConstructor=o,this._httpClient=n,this.onreceive=null,this.onclose=null,this._headers=s}async connect(n,e){Ye.isRequired(n,"url"),Ye.isRequired(e,"transferFormat"),Ye.isIn(e,Pt,"transferFormat"),this._logger.log(w.Trace,"(WebSockets transport) Connecting.");let i;return this._accessTokenFactory&&(i=await this._accessTokenFactory()),new Promise((r,o)=>{n=n.replace(/^http/,"ws");let s,a=this._httpClient.getCookieString(n),c=!1;if(Ke.isNode||Ke.isReactNative){let l={},[u,m]=nr();l[u]=m,i&&(l[Es.Authorization]=`Bearer ${i}`),a&&(l[Es.Cookie]=a),s=new this._webSocketConstructor(n,void 0,{headers:b(b({},l),this._headers)})}else i&&(n+=(n.indexOf("?")<0?"?":"&")+`access_token=${encodeURIComponent(i)}`);s||(s=new this._webSocketConstructor(n)),e===Pt.Binary&&(s.binaryType="arraybuffer"),s.onopen=l=>{this._logger.log(w.Information,`WebSocket connected to ${n}.`),this._webSocket=s,c=!0,r()},s.onerror=l=>{let u=null;typeof ErrorEvent<"u"&&l instanceof ErrorEvent?u=l.error:u="There was an error with the transport",this._logger.log(w.Information,`(WebSockets transport) ${u}.`)},s.onmessage=l=>{if(this._logger.log(w.Trace,`(WebSockets transport) data received. ${vo(l.data,this._logMessageContent)}.`),this.onreceive)try{this.onreceive(l.data)}catch(u){this._close(u);return}},s.onclose=l=>{if(c)this._close(l);else{let u=null;typeof ErrorEvent<"u"&&l instanceof ErrorEvent?u=l.error:u="WebSocket failed to connect. The connection could not be found on the server, either the endpoint may not be a SignalR endpoint, the connection ID is not present on the server, or there is a proxy blocking WebSockets. If you have multiple servers check that sticky sessions are enabled.",o(new Error(u))}}})}send(n){return this._webSocket&&this._webSocket.readyState===this._webSocketConstructor.OPEN?(this._logger.log(w.Trace,`(WebSockets transport) sending data. ${vo(n,this._logMessageContent)}.`),this._webSocket.send(n),Promise.resolve()):Promise.reject("WebSocket is not in the OPEN state")}stop(){return this._webSocket&&this._close(void 0),Promise.resolve()}_close(n){this._webSocket&&(this._webSocket.onclose=()=>{},this._webSocket.onmessage=()=>{},this._webSocket.onerror=()=>{},this._webSocket.close(),this._webSocket=void 0),this._logger.log(w.Trace,"(WebSockets transport) socket closed."),this.onclose&&(this._isCloseEvent(n)&&(n.wasClean===!1||n.code!==1e3)?this.onclose(new Error(`WebSocket closed with status code: ${n.code} (${n.reason||"no reason given"}).`)):n instanceof Error?this.onclose(n):this.onclose())}_isCloseEvent(n){return n&&typeof n.wasClean=="boolean"&&typeof n.code=="number"}};var Sk=100,Lh=class{constructor(n,e={}){if(this._stopPromiseResolver=()=>{},this.features={},this._negotiateVersion=1,Ye.isRequired(n,"url"),this._logger=wk(e.logger),this.baseUrl=this._resolveUrl(n),e=e||{},e.logMessageContent=e.logMessageContent===void 0?!1:e.logMessageContent,typeof e.withCredentials=="boolean"||e.withCredentials===void 0)e.withCredentials=e.withCredentials===void 0?!0:e.withCredentials;else throw new Error("withCredentials option was not a 'boolean' or 'undefined' value");e.timeout=e.timeout===void 0?100*1e3:e.timeout;let i=null,r=null;if(Ke.isNode&&typeof Ss<"u"){let o=typeof __webpack_require__=="function"?__non_webpack_require__:Ss;i=o("ws"),r=o("eventsource")}!Ke.isNode&&typeof WebSocket<"u"&&!e.WebSocket?e.WebSocket=WebSocket:Ke.isNode&&!e.WebSocket&&i&&(e.WebSocket=i),!Ke.isNode&&typeof EventSource<"u"&&!e.EventSource?e.EventSource=EventSource:Ke.isNode&&!e.EventSource&&typeof r<"u"&&(e.EventSource=r),this._httpClient=new Oh(e.httpClient||new Mh(this._logger),e.accessTokenFactory),this._connectionState="Disconnected",this._connectionStarted=!1,this._options=e,this.onreceive=null,this.onclose=null}async start(n){if(n=n||Pt.Binary,Ye.isIn(n,Pt,"transferFormat"),this._logger.log(w.Debug,`Starting connection with transfer format '${Pt[n]}'.`),this._connectionState!=="Disconnected")return Promise.reject(new Error("Cannot start an HttpConnection that is not in the 'Disconnected' state."));if(this._connectionState="Connecting",this._startInternalPromise=this._startInternal(n),await this._startInternalPromise,this._connectionState==="Disconnecting"){let e="Failed to start the HttpConnection before stop() was called.";return this._logger.log(w.Error,e),await this._stopPromise,Promise.reject(new Zt(e))}else if(this._connectionState!=="Connected"){let e="HttpConnection.startInternal completed gracefully but didn't enter the connection into the connected state!";return this._logger.log(w.Error,e),Promise.reject(new Zt(e))}this._connectionStarted=!0}send(n){return this._connectionState!=="Connected"?Promise.reject(new Error("Cannot send data if the connection is not in the 'Connected' State.")):(this._sendQueue||(this._sendQueue=new wy(this.transport)),this._sendQueue.send(n))}async stop(n){if(this._connectionState==="Disconnected")return this._logger.log(w.Debug,`Call to HttpConnection.stop(${n}) ignored because the connection is already in the disconnected state.`),Promise.resolve();if(this._connectionState==="Disconnecting")return this._logger.log(w.Debug,`Call to HttpConnection.stop(${n}) ignored because the connection is already in the disconnecting state.`),this._stopPromise;this._connectionState="Disconnecting",this._stopPromise=new Promise(e=>{this._stopPromiseResolver=e}),await this._stopInternal(n),await this._stopPromise}async _stopInternal(n){this._stopError=n;try{await this._startInternalPromise}catch{}if(this.transport){try{await this.transport.stop()}catch(e){this._logger.log(w.Error,`HttpConnection.transport.stop() threw error '${e}'.`),this._stopConnection()}this.transport=void 0}else this._logger.log(w.Debug,"HttpConnection.transport is undefined in HttpConnection.stop() because start() failed.")}async _startInternal(n){let e=this.baseUrl;this._accessTokenFactory=this._options.accessTokenFactory,this._httpClient._accessTokenFactory=this._accessTokenFactory;try{if(this._options.skipNegotiation)if(this._options.transport===zt.WebSockets)this.transport=this._constructTransport(zt.WebSockets),await this._startTransport(e,n);else throw new Error("Negotiation can only be skipped when using the WebSocket transport directly.");else{let i=null,r=0;do{if(i=await this._getNegotiationResponse(e),this._connectionState==="Disconnecting"||this._connectionState==="Disconnected")throw new Zt("The connection was stopped during negotiation.");if(i.error)throw new Error(i.error);if(i.ProtocolVersion)throw new Error("Detected a connection attempt to an ASP.NET SignalR Server. This client only supports connecting to an ASP.NET Core SignalR Server. See https://aka.ms/signalr-core-differences for details.");if(i.url&&(e=i.url),i.accessToken){let o=i.accessToken;this._accessTokenFactory=()=>o,this._httpClient._accessToken=o,this._httpClient._accessTokenFactory=void 0}r++}while(i.url&&r<Sk);if(r===Sk&&i.url)throw new Error("Negotiate redirection limit exceeded.");await this._createTransport(e,this._options.transport,i,n)}this.transport instanceof bd&&(this.features.inherentKeepAlive=!0),this._connectionState==="Connecting"&&(this._logger.log(w.Debug,"The HttpConnection connected successfully."),this._connectionState="Connected")}catch(i){return this._logger.log(w.Error,"Failed to start the connection: "+i),this._connectionState="Disconnected",this.transport=void 0,this._stopPromiseResolver(),Promise.reject(i)}}async _getNegotiationResponse(n){let e={},[i,r]=nr();e[i]=r;let o=this._resolveNegotiateUrl(n);this._logger.log(w.Debug,`Sending negotiation request: ${o}.`);try{let s=await this._httpClient.post(o,{content:"",headers:b(b({},e),this._options.headers),timeout:this._options.timeout,withCredentials:this._options.withCredentials});if(s.statusCode!==200)return Promise.reject(new Error(`Unexpected status code returned from negotiate '${s.statusCode}'`));let a=JSON.parse(s.content);return(!a.negotiateVersion||a.negotiateVersion<1)&&(a.connectionToken=a.connectionId),a.useStatefulReconnect&&this._options._useStatefulReconnect!==!0?Promise.reject(new gd("Client didn't negotiate Stateful Reconnect but the server did.")):a}catch(s){let a="Failed to complete negotiation with the server: "+s;return s instanceof zn&&s.statusCode===404&&(a=a+" Either this is not a SignalR endpoint or there is a proxy blocking the connection."),this._logger.log(w.Error,a),Promise.reject(new gd(a))}}_createConnectUrl(n,e){return e?n+(n.indexOf("?")===-1?"?":"&")+`id=${e}`:n}async _createTransport(n,e,i,r){let o=this._createConnectUrl(n,i.connectionToken);if(this._isITransport(e)){this._logger.log(w.Debug,"Connection was provided an instance of ITransport, using that directly."),this.transport=e,await this._startTransport(o,r),this.connectionId=i.connectionId;return}let s=[],a=i.availableTransports||[],c=i;for(let l of a){let u=this._resolveTransportOrError(l,e,r,c?.useStatefulReconnect===!0);if(u instanceof Error)s.push(`${l.transport} failed:`),s.push(u);else if(this._isITransport(u)){if(this.transport=u,!c){try{c=await this._getNegotiationResponse(n)}catch(m){return Promise.reject(m)}o=this._createConnectUrl(n,c.connectionToken)}try{await this._startTransport(o,r),this.connectionId=c.connectionId;return}catch(m){if(this._logger.log(w.Error,`Failed to start the transport '${l.transport}': ${m}`),c=void 0,s.push(new Ch(`${l.transport} failed: ${m}`,zt[l.transport])),this._connectionState!=="Connecting"){let g="Failed to select transport before stop() was called.";return this._logger.log(w.Debug,g),Promise.reject(new Zt(g))}}}}return s.length>0?Promise.reject(new wh(`Unable to connect to the server with any of the available transports. ${s.join(" ")}`,s)):Promise.reject(new Error("None of the transports supported by the client are supported by the server."))}_constructTransport(n){switch(n){case zt.WebSockets:if(!this._options.WebSocket)throw new Error("'WebSocket' is not supported in your environment.");return new Fh(this._httpClient,this._accessTokenFactory,this._logger,this._options.logMessageContent,this._options.WebSocket,this._options.headers||{});case zt.ServerSentEvents:if(!this._options.EventSource)throw new Error("'EventSource' is not supported in your environment.");return new Ph(this._httpClient,this._httpClient._accessToken,this._logger,this._options);case zt.LongPolling:return new bd(this._httpClient,this._logger,this._options);default:throw new Error(`Unknown transport: ${n}.`)}}_startTransport(n,e){return this.transport.onreceive=this.onreceive,this.features.reconnect?this.transport.onclose=async i=>{let r=!1;if(this.features.reconnect)try{this.features.disconnected(),await this.transport.connect(n,e),await this.features.resend()}catch{r=!0}else{this._stopConnection(i);return}r&&this._stopConnection(i)}:this.transport.onclose=i=>this._stopConnection(i),this.transport.connect(n,e)}_resolveTransportOrError(n,e,i,r){let o=zt[n.transport];if(o==null)return this._logger.log(w.Debug,`Skipping transport '${n.transport}' because it is not supported by this client.`),new Error(`Skipping transport '${n.transport}' because it is not supported by this client.`);if(iH(e,o))if(n.transferFormats.map(a=>Pt[a]).indexOf(i)>=0){if(o===zt.WebSockets&&!this._options.WebSocket||o===zt.ServerSentEvents&&!this._options.EventSource)return this._logger.log(w.Debug,`Skipping transport '${zt[o]}' because it is not supported in your environment.'`),new yh(`'${zt[o]}' is not supported in your environment.`,o);this._logger.log(w.Debug,`Selecting transport '${zt[o]}'.`);try{return this.features.reconnect=o===zt.WebSockets?r:void 0,this._constructTransport(o)}catch(a){return a}}else return this._logger.log(w.Debug,`Skipping transport '${zt[o]}' because it does not support the requested transfer format '${Pt[i]}'.`),new Error(`'${zt[o]}' does not support ${Pt[i]}.`);else return this._logger.log(w.Debug,`Skipping transport '${zt[o]}' because it was disabled by the client.`),new xh(`'${zt[o]}' is disabled by the client.`,o)}_isITransport(n){return n&&typeof n=="object"&&"connect"in n}_stopConnection(n){if(this._logger.log(w.Debug,`HttpConnection.stopConnection(${n}) called while in state ${this._connectionState}.`),this.transport=void 0,n=this._stopError||n,this._stopError=void 0,this._connectionState==="Disconnected"){this._logger.log(w.Debug,`Call to HttpConnection.stopConnection(${n}) was ignored because the connection is already in the disconnected state.`);return}if(this._connectionState==="Connecting")throw this._logger.log(w.Warning,`Call to HttpConnection.stopConnection(${n}) was ignored because the connection is still in the connecting state.`),new Error(`HttpConnection.stopConnection(${n}) was called while the connection is still in the connecting state.`);if(this._connectionState==="Disconnecting"&&this._stopPromiseResolver(),n?this._logger.log(w.Error,`Connection disconnected with error '${n}'.`):this._logger.log(w.Information,"Connection disconnected."),this._sendQueue&&(this._sendQueue.stop().catch(e=>{this._logger.log(w.Error,`TransportSendQueue.stop() threw error '${e}'.`)}),this._sendQueue=void 0),this.connectionId=void 0,this._connectionState="Disconnected",this._connectionStarted){this._connectionStarted=!1;try{this.onclose&&this.onclose(n)}catch(e){this._logger.log(w.Error,`HttpConnection.onclose(${n}) threw error '${e}'.`)}}}_resolveUrl(n){if(n.lastIndexOf("https://",0)===0||n.lastIndexOf("http://",0)===0)return n;if(!Ke.isBrowser)throw new Error(`Cannot resolve '${n}'.`);let e=window.document.createElement("a");return e.href=n,this._logger.log(w.Information,`Normalizing '${n}' to '${e.href}'.`),e.href}_resolveNegotiateUrl(n){let e=new URL(n);e.pathname.endsWith("/")?e.pathname+="negotiate":e.pathname+="/negotiate";let i=new URLSearchParams(e.searchParams);return i.has("negotiateVersion")||i.append("negotiateVersion",this._negotiateVersion.toString()),i.has("useStatefulReconnect")?i.get("useStatefulReconnect")==="true"&&(this._options._useStatefulReconnect=!0):this._options._useStatefulReconnect===!0&&i.append("useStatefulReconnect","true"),e.search=i.toString(),e.toString()}};function iH(t,n){return!t||(n&t)!==0}var wy=class t{constructor(n){this._transport=n,this._buffer=[],this._executing=!0,this._sendBufferedData=new Ka,this._transportResult=new Ka,this._sendLoopPromise=this._sendLoop()}send(n){return this._bufferData(n),this._transportResult||(this._transportResult=new Ka),this._transportResult.promise}stop(){return this._executing=!1,this._sendBufferedData.resolve(),this._sendLoopPromise}_bufferData(n){if(this._buffer.length&&typeof this._buffer[0]!=typeof n)throw new Error(`Expected data to be of type ${typeof this._buffer} but was of type ${typeof n}`);this._buffer.push(n),this._sendBufferedData.resolve()}async _sendLoop(){for(;;){if(await this._sendBufferedData.promise,!this._executing){this._transportResult&&this._transportResult.reject("Connection stopped.");break}this._sendBufferedData=new Ka;let n=this._transportResult;this._transportResult=void 0;let e=typeof this._buffer[0]=="string"?this._buffer.join(""):t._concatBuffers(this._buffer);this._buffer.length=0;try{await this._transport.send(e),n.resolve()}catch(i){n.reject(i)}}}static _concatBuffers(n){let e=n.map(o=>o.byteLength).reduce((o,s)=>o+s),i=new Uint8Array(e),r=0;for(let o of n)i.set(new Uint8Array(o),r),r+=o.byteLength;return i.buffer}},Ka=class{constructor(){this.promise=new Promise((n,e)=>[this._resolver,this._rejecter]=[n,e])}resolve(){this._resolver()}reject(n){this._rejecter(n)}};var rH="json",jh=class{constructor(){this.name=rH,this.version=2,this.transferFormat=Pt.Text}parseMessages(n,e){if(typeof n!="string")throw new Error("Invalid input for JSON hub protocol. Expected a string.");if(!n)return[];e===null&&(e=tr.instance);let i=$n.parse(n),r=[];for(let o of i){let s=JSON.parse(o);if(typeof s.type!="number")throw new Error("Invalid payload.");switch(s.type){case le.Invocation:this._isInvocationMessage(s);break;case le.StreamItem:this._isStreamItemMessage(s);break;case le.Completion:this._isCompletionMessage(s);break;case le.Ping:break;case le.Close:break;case le.Ack:this._isAckMessage(s);break;case le.Sequence:this._isSequenceMessage(s);break;default:e.log(w.Information,"Unknown message type '"+s.type+"' ignored.");continue}r.push(s)}return r}writeMessage(n){return $n.write(JSON.stringify(n))}_isInvocationMessage(n){this._assertNotEmptyString(n.target,"Invalid payload for Invocation message."),n.invocationId!==void 0&&this._assertNotEmptyString(n.invocationId,"Invalid payload for Invocation message.")}_isStreamItemMessage(n){if(this._assertNotEmptyString(n.invocationId,"Invalid payload for StreamItem message."),n.item===void 0)throw new Error("Invalid payload for StreamItem message.")}_isCompletionMessage(n){if(n.result&&n.error)throw new Error("Invalid payload for Completion message.");!n.result&&n.error&&this._assertNotEmptyString(n.error,"Invalid payload for Completion message."),this._assertNotEmptyString(n.invocationId,"Invalid payload for Completion message.")}_isAckMessage(n){if(typeof n.sequenceId!="number")throw new Error("Invalid SequenceId for Ack message.")}_isSequenceMessage(n){if(typeof n.sequenceId!="number")throw new Error("Invalid SequenceId for Sequence message.")}_assertNotEmptyString(n,e){if(typeof n!="string"||n==="")throw new Error(e)}};var oH={trace:w.Trace,debug:w.Debug,info:w.Information,information:w.Information,warn:w.Warning,warning:w.Warning,error:w.Error,critical:w.Critical,none:w.None};function sH(t){let n=oH[t.toLowerCase()];if(typeof n<"u")return n;throw new Error(`Unknown log level: ${t}`)}var yd=class{configureLogging(n){if(Ye.isRequired(n,"logging"),aH(n))this.logger=n;else if(typeof n=="string"){let e=sH(n);this.logger=new Ds(e)}else this.logger=new Ds(n);return this}withUrl(n,e){return Ye.isRequired(n,"url"),Ye.isNotEmpty(n,"url"),this.url=n,typeof e=="object"?this.httpConnectionOptions=b(b({},this.httpConnectionOptions),e):this.httpConnectionOptions=Z(b({},this.httpConnectionOptions),{transport:e}),this}withHubProtocol(n){return Ye.isRequired(n,"protocol"),this.protocol=n,this}withAutomaticReconnect(n){if(this.reconnectPolicy)throw new Error("A reconnectPolicy has already been set.");return n?Array.isArray(n)?this.reconnectPolicy=new vd(n):this.reconnectPolicy=n:this.reconnectPolicy=new vd,this}withServerTimeout(n){return Ye.isRequired(n,"milliseconds"),this._serverTimeoutInMilliseconds=n,this}withKeepAliveInterval(n){return Ye.isRequired(n,"milliseconds"),this._keepAliveIntervalInMilliseconds=n,this}withStatefulReconnect(n){return this.httpConnectionOptions===void 0&&(this.httpConnectionOptions={}),this.httpConnectionOptions._useStatefulReconnect=!0,this._statefulReconnectBufferSize=n?.bufferSize,this}build(){let n=this.httpConnectionOptions||{};if(n.logger===void 0&&(n.logger=this.logger),!this.url)throw new Error("The 'HubConnectionBuilder.withUrl' method must be called before building the connection.");let e=new Lh(this.url,n);return _d.create(e,this.logger||tr.instance,this.protocol||new jh,this.reconnectPolicy,this._serverTimeoutInMilliseconds,this._keepAliveIntervalInMilliseconds,this._statefulReconnectBufferSize)}};function aH(t){return t.log!==void 0}var bo=class t{hubUrl=Mr.hubUrl;hubConnection;orderSignal=M(null);createHubConnection(){this.hubConnection=new yd().withUrl(this.hubUrl,{withCredentials:!0}).withAutomaticReconnect().build(),this.hubConnection.start().catch(n=>console.error(n)),this.hubConnection.on("OrderCompletedNotification",n=>{this.orderSignal.set(n)})}stopHubConnection(){this.hubConnection?.state===Je.Connected&&this.hubConnection.stop().catch(n=>console.error(n))}static \u0275fac=function(e){return new(e||t)};static \u0275prov=C({token:t,factory:t.\u0275fac,providedIn:"root"})};var dn=class t{baseUrl=oo.apiUrl;http=d(Wt);signalrService=d(bo);currentUser=M(null);login(n){let e=new jn;return e=e.append("useCookies","true"),this.http.post(this.baseUrl+"login",n,{params:e}).pipe(st(()=>this.signalrService.createHubConnection()))}register(n){return this.http.post(this.baseUrl+"account/register",n)}getUserInfo(){return this.http.get(this.baseUrl+"account/user-info").pipe(Q(n=>(this.currentUser.set(n),n)))}logout(){return this.http.post(this.baseUrl+"account/logout",{}).pipe(st(()=>this.signalrService.stopHubConnection()))}updateAddress(n){return this.http.post(this.baseUrl+"account/address",n).pipe(st(()=>{this.currentUser.update(e=>(e&&(e.address=n),e))}))}getAuthState(){return this.http.get(this.baseUrl+"account/auth-status")}static \u0275fac=function(e){return new(e||t)};static \u0275prov=C({token:t,factory:t.\u0275fac,providedIn:"root"})};var Bh=class t{baseUrl=Mr.apiUrl;http=d(Wt);accountService=d(dn);cartService=d(rt);stripePromise;elements;addressElement;paymentElement;constructor(){this.stripePromise=xk(Mr.stripePublicKey)}getStripeInstance(){return this.stripePromise}createOrUpdatePaymentIntent(){let n=this.cartService.cart();if(!n)throw new Error("Problem with cart");return this.http.post(this.baseUrl+"payments/"+n.id,{}).pipe(Q(e=>(this.cartService.setCart(e),e)))}async createConfirmationToken(){let n=await this.getStripeInstance(),e=await this.initializeElements(),i=await e.submit();if(i.error)throw new Error(i.error.message);if(n)return await n.createConfirmationToken({elements:e});throw new Error("Stripe not available")}async confirmPayment(n){let e=await this.getStripeInstance(),r=await(await this.initializeElements()).submit();if(r.error)throw new Error(r.error.message);let o=this.cartService.cart()?.clientSecret;if(!o)throw new Error("Client secret not found");if(!e)throw new Error("Stripe not available");return await e.confirmPayment({clientSecret:o,confirmParams:{confirmation_token:n.id},redirect:"if_required"})}async initializeElements(){if(!this.elements){let n=await this.getStripeInstance();if(n){let e=await Ro(this.createOrUpdatePaymentIntent());this.elements=n.elements({clientSecret:e.clientSecret,appearance:{labels:"floating"}})}else throw new Error("Stripe has not been loaded")}return this.elements}async createAddressElement(){if(!this.addressElement){let n=await this.initializeElements();if(n){let e=this.accountService.currentUser(),i={};e&&(i.name=e.firstName+" "+e.lastName),e?.address&&(i.address={line1:e.address.line1,line2:e.address.line2,city:e.address.city,state:e.address.state,postal_code:e.address.postalCode,country:e.address.country});let r={mode:"shipping",defaultValues:i};this.addressElement=n.create("address",r)}else throw new Error("Elements instance has not been loaded")}return this.addressElement}async createPaymentElement(){if(!this.paymentElement){let n=await this.initializeElements();if(n)this.paymentElement=n.create("payment");else throw new Error("Elements instance has not been loaded")}return this.paymentElement}disposeElements(){this.elements=void 0,this.addressElement=void 0,this.paymentElement=void 0}static \u0275fac=function(e){return new(e||t)};static \u0275prov=C({token:t,factory:t.\u0275fac,providedIn:"root"})};var cH=["mat-internal-form-field",""],lH=["*"],Vh=(()=>{class t{labelPosition="after";static \u0275fac=function(i){return new(i||t)};static \u0275cmp=E({type:t,selectors:[["div","mat-internal-form-field",""]],hostAttrs:[1,"mdc-form-field","mat-internal-form-field"],hostVars:2,hostBindings:function(i,r){i&2&&z("mdc-form-field--align-end",r.labelPosition==="before")},inputs:{labelPosition:"labelPosition"},attrs:cH,ngContentSelectors:lH,decls:1,vars:0,template:function(i,r){i&1&&(Ce(),re(0))},styles:[`.mat-internal-form-field {
  -moz-osx-font-smoothing: grayscale;
  -webkit-font-smoothing: antialiased;
  display: inline-flex;
  align-items: center;
  vertical-align: middle;
}
.mat-internal-form-field > label {
  margin-left: 0;
  margin-right: auto;
  padding-left: 4px;
  padding-right: 0;
  order: 0;
}
[dir=rtl] .mat-internal-form-field > label {
  margin-left: auto;
  margin-right: 0;
  padding-left: 0;
  padding-right: 4px;
}

.mdc-form-field--align-end > label {
  margin-left: auto;
  margin-right: 0;
  padding-left: 0;
  padding-right: 4px;
  order: -1;
}
[dir=rtl] .mdc-form-field--align-end .mdc-form-field--align-end label {
  margin-left: 0;
  margin-right: auto;
  padding-left: 4px;
  padding-right: 0;
}
`],encapsulation:2,changeDetection:0})}return t})();var dH=["input"],uH=["label"],mH=["*"],Dy={color:"accent",clickAction:"check-indeterminate",disabledInteractive:!1},fH=new y("mat-checkbox-default-options",{providedIn:"root",factory:()=>Dy}),un=(function(t){return t[t.Init=0]="Init",t[t.Checked=1]="Checked",t[t.Unchecked=2]="Unchecked",t[t.Indeterminate=3]="Indeterminate",t})(un||{}),Ey=class{source;checked},Sy=(()=>{class t{_elementRef=d(L);_changeDetectorRef=d(ve);_ngZone=d(H);_animationsDisabled=ke();_options=d(fH,{optional:!0});focus(){this._inputElement.nativeElement.focus()}_createChangeEvent(e){let i=new Ey;return i.source=this,i.checked=e,i}_getAnimationTargetElement(){return this._inputElement?.nativeElement}_animationClasses={uncheckedToChecked:"mdc-checkbox--anim-unchecked-checked",uncheckedToIndeterminate:"mdc-checkbox--anim-unchecked-indeterminate",checkedToUnchecked:"mdc-checkbox--anim-checked-unchecked",checkedToIndeterminate:"mdc-checkbox--anim-checked-indeterminate",indeterminateToChecked:"mdc-checkbox--anim-indeterminate-checked",indeterminateToUnchecked:"mdc-checkbox--anim-indeterminate-unchecked"};ariaLabel="";ariaLabelledby=null;ariaDescribedby;ariaExpanded;ariaControls;ariaOwns;_uniqueId;id;get inputId(){return`${this.id||this._uniqueId}-input`}required=!1;labelPosition="after";name=null;change=new U;indeterminateChange=new U;value;disableRipple=!1;_inputElement;_labelElement;tabIndex;color;disabledInteractive;_onTouched=()=>{};_currentAnimationClass="";_currentCheckState=un.Init;_controlValueAccessorChangeFn=()=>{};_validatorChangeFn=()=>{};constructor(){d(qe).load(An);let e=d(new Kn("tabindex"),{optional:!0});this._options=this._options||Dy,this.color=this._options.color||Dy.color,this.tabIndex=e==null?0:parseInt(e)||0,this.id=this._uniqueId=d(Ie).getId("mat-mdc-checkbox-"),this.disabledInteractive=this._options?.disabledInteractive??!1}ngOnChanges(e){e.required&&this._validatorChangeFn()}ngAfterViewInit(){this._syncIndeterminate(this.indeterminate)}get checked(){return this._checked}set checked(e){e!=this.checked&&(this._checked=e,this._changeDetectorRef.markForCheck())}_checked=!1;get disabled(){return this._disabled}set disabled(e){e!==this.disabled&&(this._disabled=e,this._changeDetectorRef.markForCheck())}_disabled=!1;get indeterminate(){return this._indeterminate()}set indeterminate(e){let i=e!=this._indeterminate();this._indeterminate.set(e),i&&(e?this._transitionCheckState(un.Indeterminate):this._transitionCheckState(this.checked?un.Checked:un.Unchecked),this.indeterminateChange.emit(e)),this._syncIndeterminate(e)}_indeterminate=M(!1);_isRippleDisabled(){return this.disableRipple||this.disabled}_onLabelTextChange(){this._changeDetectorRef.detectChanges()}writeValue(e){this.checked=!!e}registerOnChange(e){this._controlValueAccessorChangeFn=e}registerOnTouched(e){this._onTouched=e}setDisabledState(e){this.disabled=e}validate(e){return this.required&&e.value!==!0?{required:!0}:null}registerOnValidatorChange(e){this._validatorChangeFn=e}_transitionCheckState(e){let i=this._currentCheckState,r=this._getAnimationTargetElement();if(!(i===e||!r)&&(this._currentAnimationClass&&r.classList.remove(this._currentAnimationClass),this._currentAnimationClass=this._getAnimationClassForCheckStateTransition(i,e),this._currentCheckState=e,this._currentAnimationClass.length>0)){r.classList.add(this._currentAnimationClass);let o=this._currentAnimationClass;this._ngZone.runOutsideAngular(()=>{setTimeout(()=>{r.classList.remove(o)},1e3)})}}_emitChangeEvent(){this._controlValueAccessorChangeFn(this.checked),this.change.emit(this._createChangeEvent(this.checked)),this._inputElement&&(this._inputElement.nativeElement.checked=this.checked)}toggle(){this.checked=!this.checked,this._controlValueAccessorChangeFn(this.checked)}_handleInputClick(){let e=this._options?.clickAction;!this.disabled&&e!=="noop"?(this.indeterminate&&e!=="check"&&Promise.resolve().then(()=>{this._indeterminate.set(!1),this.indeterminateChange.emit(!1)}),this._checked=!this._checked,this._transitionCheckState(this._checked?un.Checked:un.Unchecked),this._emitChangeEvent()):(this.disabled&&this.disabledInteractive||!this.disabled&&e==="noop")&&(this._inputElement.nativeElement.checked=this.checked,this._inputElement.nativeElement.indeterminate=this.indeterminate)}_onInteractionEvent(e){e.stopPropagation()}_onBlur(){Promise.resolve().then(()=>{this._onTouched(),this._changeDetectorRef.markForCheck()})}_getAnimationClassForCheckStateTransition(e,i){if(this._animationsDisabled)return"";switch(e){case un.Init:if(i===un.Checked)return this._animationClasses.uncheckedToChecked;if(i==un.Indeterminate)return this._checked?this._animationClasses.checkedToIndeterminate:this._animationClasses.uncheckedToIndeterminate;break;case un.Unchecked:return i===un.Checked?this._animationClasses.uncheckedToChecked:this._animationClasses.uncheckedToIndeterminate;case un.Checked:return i===un.Unchecked?this._animationClasses.checkedToUnchecked:this._animationClasses.checkedToIndeterminate;case un.Indeterminate:return i===un.Checked?this._animationClasses.indeterminateToChecked:this._animationClasses.indeterminateToUnchecked}return""}_syncIndeterminate(e){let i=this._inputElement;i&&(i.nativeElement.indeterminate=e)}_onInputClick(){this._handleInputClick()}_onTouchTargetClick(){this._handleInputClick(),this.disabled||this._inputElement.nativeElement.focus()}_preventBubblingFromLabel(e){e.target&&this._labelElement.nativeElement.contains(e.target)&&e.stopPropagation()}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=E({type:t,selectors:[["mat-checkbox"]],viewQuery:function(i,r){if(i&1&&Ve(dH,5)(uH,5),i&2){let o;$(o=G())&&(r._inputElement=o.first),$(o=G())&&(r._labelElement=o.first)}},hostAttrs:[1,"mat-mdc-checkbox"],hostVars:16,hostBindings:function(i,r){i&2&&(mt("id",r.id),q("tabindex",null)("aria-label",null)("aria-labelledby",null),Tt(r.color?"mat-"+r.color:"mat-accent"),z("_mat-animation-noopable",r._animationsDisabled)("mdc-checkbox--disabled",r.disabled)("mat-mdc-checkbox-disabled",r.disabled)("mat-mdc-checkbox-checked",r.checked)("mat-mdc-checkbox-disabled-interactive",r.disabledInteractive))},inputs:{ariaLabel:[0,"aria-label","ariaLabel"],ariaLabelledby:[0,"aria-labelledby","ariaLabelledby"],ariaDescribedby:[0,"aria-describedby","ariaDescribedby"],ariaExpanded:[2,"aria-expanded","ariaExpanded",B],ariaControls:[0,"aria-controls","ariaControls"],ariaOwns:[0,"aria-owns","ariaOwns"],id:"id",required:[2,"required","required",B],labelPosition:"labelPosition",name:"name",value:"value",disableRipple:[2,"disableRipple","disableRipple",B],tabIndex:[2,"tabIndex","tabIndex",e=>e==null?void 0:lt(e)],color:"color",disabledInteractive:[2,"disabledInteractive","disabledInteractive",B],checked:[2,"checked","checked",B],disabled:[2,"disabled","disabled",B],indeterminate:[2,"indeterminate","indeterminate",B]},outputs:{change:"change",indeterminateChange:"indeterminateChange"},exportAs:["matCheckbox"],features:[Ee([{provide:Ei,useExisting:It(()=>t),multi:!0},{provide:Si,useExisting:t,multi:!0}]),ze],ngContentSelectors:mH,decls:15,vars:23,consts:[["checkbox",""],["input",""],["label",""],["mat-internal-form-field","",3,"click","labelPosition"],[1,"mdc-checkbox"],["aria-hidden","true",1,"mat-mdc-checkbox-touch-target",3,"click"],["type","checkbox",1,"mdc-checkbox__native-control",3,"blur","click","change","checked","indeterminate","disabled","id","required","tabIndex"],["aria-hidden","true",1,"mdc-checkbox__ripple"],["aria-hidden","true",1,"mdc-checkbox__background"],["focusable","false","viewBox","0 0 24 24",1,"mdc-checkbox__checkmark"],["fill","none","d","M1.73,12.91 8.1,19.28 22.79,4.59",1,"mdc-checkbox__checkmark-path"],[1,"mdc-checkbox__mixedmark"],["mat-ripple","","aria-hidden","true",1,"mat-mdc-checkbox-ripple","mat-focus-indicator",3,"matRippleTrigger","matRippleDisabled","matRippleCentered"],[1,"mdc-label",3,"for"]],template:function(i,r){if(i&1&&(Ce(),f(0,"div",3),T("click",function(s){return r._preventBubblingFromLabel(s)}),f(1,"div",4,0)(3,"div",5),T("click",function(){return r._onTouchTargetClick()}),h(),f(4,"input",6,1),T("blur",function(){return r._onBlur()})("click",function(){return r._onInputClick()})("change",function(s){return r._onInteractionEvent(s)}),h(),O(6,"div",7),f(7,"div",8),Gt(),f(8,"svg",9),O(9,"path",10),h(),Vi(),O(10,"div",11),h(),O(11,"div",12),h(),f(12,"label",13,2),re(14),h()()),i&2){let o=Re(2);D("labelPosition",r.labelPosition),p(4),z("mdc-checkbox--selected",r.checked),D("checked",r.checked)("indeterminate",r.indeterminate)("disabled",r.disabled&&!r.disabledInteractive)("id",r.inputId)("required",r.required)("tabIndex",r.disabled&&!r.disabledInteractive?-1:r.tabIndex),q("aria-label",r.ariaLabel||null)("aria-labelledby",r.ariaLabelledby)("aria-describedby",r.ariaDescribedby)("aria-checked",r.indeterminate?"mixed":null)("aria-controls",r.ariaControls)("aria-disabled",r.disabled&&r.disabledInteractive?!0:null)("aria-expanded",r.ariaExpanded)("aria-owns",r.ariaOwns)("name",r.name)("value",r.value),p(7),D("matRippleTrigger",o)("matRippleDisabled",r.disableRipple||r.disabled)("matRippleCentered",!0),p(),D("for",r.inputId)}},dependencies:[Xi,Vh],styles:[`.mdc-checkbox {
  display: inline-block;
  position: relative;
  flex: 0 0 18px;
  box-sizing: content-box;
  width: 18px;
  height: 18px;
  line-height: 0;
  white-space: nowrap;
  cursor: pointer;
  vertical-align: bottom;
  padding: calc((var(--mat-checkbox-state-layer-size, 40px) - 18px) / 2);
  margin: calc((var(--mat-checkbox-state-layer-size, 40px) - var(--mat-checkbox-state-layer-size, 40px)) / 2);
}
.mdc-checkbox:hover > .mdc-checkbox__ripple {
  opacity: var(--mat-checkbox-unselected-hover-state-layer-opacity, var(--mat-sys-hover-state-layer-opacity));
  background-color: var(--mat-checkbox-unselected-hover-state-layer-color, var(--mat-sys-on-surface));
}
.mdc-checkbox:hover > .mat-mdc-checkbox-ripple > .mat-ripple-element {
  background-color: var(--mat-checkbox-unselected-hover-state-layer-color, var(--mat-sys-on-surface));
}
.mdc-checkbox .mdc-checkbox__native-control:focus + .mdc-checkbox__ripple {
  opacity: var(--mat-checkbox-unselected-focus-state-layer-opacity, var(--mat-sys-focus-state-layer-opacity));
  background-color: var(--mat-checkbox-unselected-focus-state-layer-color, var(--mat-sys-on-surface));
}
.mdc-checkbox .mdc-checkbox__native-control:focus ~ .mat-mdc-checkbox-ripple .mat-ripple-element {
  background-color: var(--mat-checkbox-unselected-focus-state-layer-color, var(--mat-sys-on-surface));
}
.mdc-checkbox:active > .mdc-checkbox__native-control + .mdc-checkbox__ripple {
  opacity: var(--mat-checkbox-unselected-pressed-state-layer-opacity, var(--mat-sys-pressed-state-layer-opacity));
  background-color: var(--mat-checkbox-unselected-pressed-state-layer-color, var(--mat-sys-primary));
}
.mdc-checkbox:active > .mdc-checkbox__native-control ~ .mat-mdc-checkbox-ripple .mat-ripple-element {
  background-color: var(--mat-checkbox-unselected-pressed-state-layer-color, var(--mat-sys-primary));
}
.mdc-checkbox:hover > .mdc-checkbox__native-control:checked + .mdc-checkbox__ripple {
  opacity: var(--mat-checkbox-selected-hover-state-layer-opacity, var(--mat-sys-hover-state-layer-opacity));
  background-color: var(--mat-checkbox-selected-hover-state-layer-color, var(--mat-sys-primary));
}
.mdc-checkbox:hover > .mdc-checkbox__native-control:checked ~ .mat-mdc-checkbox-ripple .mat-ripple-element {
  background-color: var(--mat-checkbox-selected-hover-state-layer-color, var(--mat-sys-primary));
}
.mdc-checkbox .mdc-checkbox__native-control:focus:checked + .mdc-checkbox__ripple {
  opacity: var(--mat-checkbox-selected-focus-state-layer-opacity, var(--mat-sys-focus-state-layer-opacity));
  background-color: var(--mat-checkbox-selected-focus-state-layer-color, var(--mat-sys-primary));
}
.mdc-checkbox .mdc-checkbox__native-control:focus:checked ~ .mat-mdc-checkbox-ripple .mat-ripple-element {
  background-color: var(--mat-checkbox-selected-focus-state-layer-color, var(--mat-sys-primary));
}
.mdc-checkbox:active > .mdc-checkbox__native-control:checked + .mdc-checkbox__ripple {
  opacity: var(--mat-checkbox-selected-pressed-state-layer-opacity, var(--mat-sys-pressed-state-layer-opacity));
  background-color: var(--mat-checkbox-selected-pressed-state-layer-color, var(--mat-sys-on-surface));
}
.mdc-checkbox:active > .mdc-checkbox__native-control:checked ~ .mat-mdc-checkbox-ripple .mat-ripple-element {
  background-color: var(--mat-checkbox-selected-pressed-state-layer-color, var(--mat-sys-on-surface));
}
.mdc-checkbox--disabled.mat-mdc-checkbox-disabled-interactive .mdc-checkbox .mdc-checkbox__native-control ~ .mat-mdc-checkbox-ripple .mat-ripple-element,
.mdc-checkbox--disabled.mat-mdc-checkbox-disabled-interactive .mdc-checkbox .mdc-checkbox__native-control + .mdc-checkbox__ripple {
  background-color: var(--mat-checkbox-unselected-hover-state-layer-color, var(--mat-sys-on-surface));
}
.mdc-checkbox .mdc-checkbox__native-control {
  position: absolute;
  margin: 0;
  padding: 0;
  opacity: 0;
  cursor: inherit;
  z-index: 1;
  width: var(--mat-checkbox-state-layer-size, 40px);
  height: var(--mat-checkbox-state-layer-size, 40px);
  top: calc((var(--mat-checkbox-state-layer-size, 40px) - var(--mat-checkbox-state-layer-size, 40px)) / 2);
  right: calc((var(--mat-checkbox-state-layer-size, 40px) - var(--mat-checkbox-state-layer-size, 40px)) / 2);
  left: calc((var(--mat-checkbox-state-layer-size, 40px) - var(--mat-checkbox-state-layer-size, 40px)) / 2);
}

.mdc-checkbox--disabled {
  cursor: default;
  pointer-events: none;
}

.mdc-checkbox__background {
  display: inline-flex;
  position: absolute;
  align-items: center;
  justify-content: center;
  box-sizing: border-box;
  width: 18px;
  height: 18px;
  border: 2px solid currentColor;
  border-radius: 2px;
  background-color: transparent;
  pointer-events: none;
  will-change: background-color, border-color;
  transition: background-color 90ms cubic-bezier(0.4, 0, 0.6, 1), border-color 90ms cubic-bezier(0.4, 0, 0.6, 1);
  -webkit-print-color-adjust: exact;
  color-adjust: exact;
  border-color: var(--mat-checkbox-unselected-icon-color, var(--mat-sys-on-surface-variant));
  top: calc((var(--mat-checkbox-state-layer-size, 40px) - 18px) / 2);
  left: calc((var(--mat-checkbox-state-layer-size, 40px) - 18px) / 2);
}

.mdc-checkbox__native-control:enabled:checked ~ .mdc-checkbox__background,
.mdc-checkbox__native-control:enabled:indeterminate ~ .mdc-checkbox__background {
  border-color: var(--mat-checkbox-selected-icon-color, var(--mat-sys-primary));
  background-color: var(--mat-checkbox-selected-icon-color, var(--mat-sys-primary));
}

.mdc-checkbox--disabled .mdc-checkbox__background {
  border-color: var(--mat-checkbox-disabled-unselected-icon-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
}
@media (forced-colors: active) {
  .mdc-checkbox--disabled .mdc-checkbox__background {
    border-color: GrayText;
  }
}

.mdc-checkbox__native-control:disabled:checked ~ .mdc-checkbox__background,
.mdc-checkbox__native-control:disabled:indeterminate ~ .mdc-checkbox__background {
  background-color: var(--mat-checkbox-disabled-selected-icon-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
  border-color: transparent;
}
@media (forced-colors: active) {
  .mdc-checkbox__native-control:disabled:checked ~ .mdc-checkbox__background,
  .mdc-checkbox__native-control:disabled:indeterminate ~ .mdc-checkbox__background {
    border-color: GrayText;
  }
}

.mdc-checkbox:hover > .mdc-checkbox__native-control:not(:checked) ~ .mdc-checkbox__background,
.mdc-checkbox:hover > .mdc-checkbox__native-control:not(:indeterminate) ~ .mdc-checkbox__background {
  border-color: var(--mat-checkbox-unselected-hover-icon-color, var(--mat-sys-on-surface));
  background-color: transparent;
}

.mdc-checkbox:hover > .mdc-checkbox__native-control:checked ~ .mdc-checkbox__background,
.mdc-checkbox:hover > .mdc-checkbox__native-control:indeterminate ~ .mdc-checkbox__background {
  border-color: var(--mat-checkbox-selected-hover-icon-color, var(--mat-sys-primary));
  background-color: var(--mat-checkbox-selected-hover-icon-color, var(--mat-sys-primary));
}

.mdc-checkbox__native-control:focus:focus:not(:checked) ~ .mdc-checkbox__background,
.mdc-checkbox__native-control:focus:focus:not(:indeterminate) ~ .mdc-checkbox__background {
  border-color: var(--mat-checkbox-unselected-focus-icon-color, var(--mat-sys-on-surface));
}

.mdc-checkbox__native-control:focus:focus:checked ~ .mdc-checkbox__background,
.mdc-checkbox__native-control:focus:focus:indeterminate ~ .mdc-checkbox__background {
  border-color: var(--mat-checkbox-selected-focus-icon-color, var(--mat-sys-primary));
  background-color: var(--mat-checkbox-selected-focus-icon-color, var(--mat-sys-primary));
}

.mdc-checkbox--disabled.mat-mdc-checkbox-disabled-interactive .mdc-checkbox:hover > .mdc-checkbox__native-control ~ .mdc-checkbox__background,
.mdc-checkbox--disabled.mat-mdc-checkbox-disabled-interactive .mdc-checkbox .mdc-checkbox__native-control:focus ~ .mdc-checkbox__background,
.mdc-checkbox--disabled.mat-mdc-checkbox-disabled-interactive .mdc-checkbox__background {
  border-color: var(--mat-checkbox-disabled-unselected-icon-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
}
@media (forced-colors: active) {
  .mdc-checkbox--disabled.mat-mdc-checkbox-disabled-interactive .mdc-checkbox:hover > .mdc-checkbox__native-control ~ .mdc-checkbox__background,
  .mdc-checkbox--disabled.mat-mdc-checkbox-disabled-interactive .mdc-checkbox .mdc-checkbox__native-control:focus ~ .mdc-checkbox__background,
  .mdc-checkbox--disabled.mat-mdc-checkbox-disabled-interactive .mdc-checkbox__background {
    border-color: GrayText;
  }
}
.mdc-checkbox--disabled.mat-mdc-checkbox-disabled-interactive .mdc-checkbox__native-control:checked ~ .mdc-checkbox__background,
.mdc-checkbox--disabled.mat-mdc-checkbox-disabled-interactive .mdc-checkbox__native-control:indeterminate ~ .mdc-checkbox__background {
  background-color: var(--mat-checkbox-disabled-selected-icon-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
  border-color: transparent;
}

.mdc-checkbox__checkmark {
  position: absolute;
  top: 0;
  right: 0;
  bottom: 0;
  left: 0;
  width: 100%;
  opacity: 0;
  transition: opacity 180ms cubic-bezier(0.4, 0, 0.6, 1);
  color: var(--mat-checkbox-selected-checkmark-color, var(--mat-sys-on-primary));
}
@media (forced-colors: active) {
  .mdc-checkbox__checkmark {
    color: CanvasText;
  }
}

.mdc-checkbox--disabled .mdc-checkbox__checkmark, .mdc-checkbox--disabled.mat-mdc-checkbox-disabled-interactive .mdc-checkbox__checkmark {
  color: var(--mat-checkbox-disabled-selected-checkmark-color, var(--mat-sys-surface));
}
@media (forced-colors: active) {
  .mdc-checkbox--disabled .mdc-checkbox__checkmark, .mdc-checkbox--disabled.mat-mdc-checkbox-disabled-interactive .mdc-checkbox__checkmark {
    color: GrayText;
  }
}

.mdc-checkbox__checkmark-path {
  transition: stroke-dashoffset 180ms cubic-bezier(0.4, 0, 0.6, 1);
  stroke: currentColor;
  stroke-width: 3.12px;
  stroke-dashoffset: 29.7833385;
  stroke-dasharray: 29.7833385;
}

.mdc-checkbox__mixedmark {
  width: 100%;
  height: 0;
  transform: scaleX(0) rotate(0deg);
  border-width: 1px;
  border-style: solid;
  opacity: 0;
  transition: opacity 90ms cubic-bezier(0.4, 0, 0.6, 1), transform 90ms cubic-bezier(0.4, 0, 0.6, 1);
  border-color: var(--mat-checkbox-selected-checkmark-color, var(--mat-sys-on-primary));
}
@media (forced-colors: active) {
  .mdc-checkbox__mixedmark {
    margin: 0 1px;
  }
}

.mdc-checkbox--disabled .mdc-checkbox__mixedmark, .mdc-checkbox--disabled.mat-mdc-checkbox-disabled-interactive .mdc-checkbox__mixedmark {
  border-color: var(--mat-checkbox-disabled-selected-checkmark-color, var(--mat-sys-surface));
}
@media (forced-colors: active) {
  .mdc-checkbox--disabled .mdc-checkbox__mixedmark, .mdc-checkbox--disabled.mat-mdc-checkbox-disabled-interactive .mdc-checkbox__mixedmark {
    border-color: GrayText;
  }
}

.mdc-checkbox--anim-unchecked-checked .mdc-checkbox__background,
.mdc-checkbox--anim-unchecked-indeterminate .mdc-checkbox__background,
.mdc-checkbox--anim-checked-unchecked .mdc-checkbox__background,
.mdc-checkbox--anim-indeterminate-unchecked .mdc-checkbox__background {
  animation-duration: 180ms;
  animation-timing-function: linear;
}

.mdc-checkbox--anim-unchecked-checked .mdc-checkbox__checkmark-path {
  animation: mdc-checkbox-unchecked-checked-checkmark-path 180ms linear;
  transition: none;
}

.mdc-checkbox--anim-unchecked-indeterminate .mdc-checkbox__mixedmark {
  animation: mdc-checkbox-unchecked-indeterminate-mixedmark 90ms linear;
  transition: none;
}

.mdc-checkbox--anim-checked-unchecked .mdc-checkbox__checkmark-path {
  animation: mdc-checkbox-checked-unchecked-checkmark-path 90ms linear;
  transition: none;
}

.mdc-checkbox--anim-checked-indeterminate .mdc-checkbox__checkmark {
  animation: mdc-checkbox-checked-indeterminate-checkmark 90ms linear;
  transition: none;
}
.mdc-checkbox--anim-checked-indeterminate .mdc-checkbox__mixedmark {
  animation: mdc-checkbox-checked-indeterminate-mixedmark 90ms linear;
  transition: none;
}

.mdc-checkbox--anim-indeterminate-checked .mdc-checkbox__checkmark {
  animation: mdc-checkbox-indeterminate-checked-checkmark 500ms linear;
  transition: none;
}
.mdc-checkbox--anim-indeterminate-checked .mdc-checkbox__mixedmark {
  animation: mdc-checkbox-indeterminate-checked-mixedmark 500ms linear;
  transition: none;
}

.mdc-checkbox--anim-indeterminate-unchecked .mdc-checkbox__mixedmark {
  animation: mdc-checkbox-indeterminate-unchecked-mixedmark 300ms linear;
  transition: none;
}

.mdc-checkbox__native-control:checked ~ .mdc-checkbox__background,
.mdc-checkbox__native-control:indeterminate ~ .mdc-checkbox__background {
  transition: border-color 90ms cubic-bezier(0, 0, 0.2, 1), background-color 90ms cubic-bezier(0, 0, 0.2, 1);
}
.mdc-checkbox__native-control:checked ~ .mdc-checkbox__background > .mdc-checkbox__checkmark > .mdc-checkbox__checkmark-path,
.mdc-checkbox__native-control:indeterminate ~ .mdc-checkbox__background > .mdc-checkbox__checkmark > .mdc-checkbox__checkmark-path {
  stroke-dashoffset: 0;
}

.mdc-checkbox__native-control:checked ~ .mdc-checkbox__background > .mdc-checkbox__checkmark {
  transition: opacity 180ms cubic-bezier(0, 0, 0.2, 1), transform 180ms cubic-bezier(0, 0, 0.2, 1);
  opacity: 1;
}
.mdc-checkbox__native-control:checked ~ .mdc-checkbox__background > .mdc-checkbox__mixedmark {
  transform: scaleX(1) rotate(-45deg);
}

.mdc-checkbox__native-control:indeterminate ~ .mdc-checkbox__background > .mdc-checkbox__checkmark {
  transform: rotate(45deg);
  opacity: 0;
  transition: opacity 90ms cubic-bezier(0.4, 0, 0.6, 1), transform 90ms cubic-bezier(0.4, 0, 0.6, 1);
}
.mdc-checkbox__native-control:indeterminate ~ .mdc-checkbox__background > .mdc-checkbox__mixedmark {
  transform: scaleX(1) rotate(0deg);
  opacity: 1;
}

@keyframes mdc-checkbox-unchecked-checked-checkmark-path {
  0%, 50% {
    stroke-dashoffset: 29.7833385;
  }
  50% {
    animation-timing-function: cubic-bezier(0, 0, 0.2, 1);
  }
  100% {
    stroke-dashoffset: 0;
  }
}
@keyframes mdc-checkbox-unchecked-indeterminate-mixedmark {
  0%, 68.2% {
    transform: scaleX(0);
  }
  68.2% {
    animation-timing-function: cubic-bezier(0, 0, 0, 1);
  }
  100% {
    transform: scaleX(1);
  }
}
@keyframes mdc-checkbox-checked-unchecked-checkmark-path {
  from {
    animation-timing-function: cubic-bezier(0.4, 0, 1, 1);
    opacity: 1;
    stroke-dashoffset: 0;
  }
  to {
    opacity: 0;
    stroke-dashoffset: -29.7833385;
  }
}
@keyframes mdc-checkbox-checked-indeterminate-checkmark {
  from {
    animation-timing-function: cubic-bezier(0, 0, 0.2, 1);
    transform: rotate(0deg);
    opacity: 1;
  }
  to {
    transform: rotate(45deg);
    opacity: 0;
  }
}
@keyframes mdc-checkbox-indeterminate-checked-checkmark {
  from {
    animation-timing-function: cubic-bezier(0.14, 0, 0, 1);
    transform: rotate(45deg);
    opacity: 0;
  }
  to {
    transform: rotate(360deg);
    opacity: 1;
  }
}
@keyframes mdc-checkbox-checked-indeterminate-mixedmark {
  from {
    animation-timing-function: cubic-bezier(0, 0, 0.2, 1);
    transform: rotate(-45deg);
    opacity: 0;
  }
  to {
    transform: rotate(0deg);
    opacity: 1;
  }
}
@keyframes mdc-checkbox-indeterminate-checked-mixedmark {
  from {
    animation-timing-function: cubic-bezier(0.14, 0, 0, 1);
    transform: rotate(0deg);
    opacity: 1;
  }
  to {
    transform: rotate(315deg);
    opacity: 0;
  }
}
@keyframes mdc-checkbox-indeterminate-unchecked-mixedmark {
  0% {
    animation-timing-function: linear;
    transform: scaleX(1);
    opacity: 1;
  }
  32.8%, 100% {
    transform: scaleX(0);
    opacity: 0;
  }
}
.mat-mdc-checkbox {
  display: inline-block;
  position: relative;
  -webkit-tap-highlight-color: transparent;
}
.mat-mdc-checkbox._mat-animation-noopable > .mat-internal-form-field > .mdc-checkbox > .mat-mdc-checkbox-touch-target,
.mat-mdc-checkbox._mat-animation-noopable > .mat-internal-form-field > .mdc-checkbox > .mdc-checkbox__native-control,
.mat-mdc-checkbox._mat-animation-noopable > .mat-internal-form-field > .mdc-checkbox > .mdc-checkbox__ripple,
.mat-mdc-checkbox._mat-animation-noopable > .mat-internal-form-field > .mdc-checkbox > .mat-mdc-checkbox-ripple::before,
.mat-mdc-checkbox._mat-animation-noopable > .mat-internal-form-field > .mdc-checkbox > .mdc-checkbox__background,
.mat-mdc-checkbox._mat-animation-noopable > .mat-internal-form-field > .mdc-checkbox > .mdc-checkbox__background > .mdc-checkbox__checkmark,
.mat-mdc-checkbox._mat-animation-noopable > .mat-internal-form-field > .mdc-checkbox > .mdc-checkbox__background > .mdc-checkbox__checkmark > .mdc-checkbox__checkmark-path,
.mat-mdc-checkbox._mat-animation-noopable > .mat-internal-form-field > .mdc-checkbox > .mdc-checkbox__background > .mdc-checkbox__mixedmark {
  transition: none !important;
  animation: none !important;
}
.mat-mdc-checkbox label {
  cursor: pointer;
}
.mat-mdc-checkbox .mat-internal-form-field {
  color: var(--mat-checkbox-label-text-color, var(--mat-sys-on-surface));
  font-family: var(--mat-checkbox-label-text-font, var(--mat-sys-body-medium-font));
  line-height: var(--mat-checkbox-label-text-line-height, var(--mat-sys-body-medium-line-height));
  font-size: var(--mat-checkbox-label-text-size, var(--mat-sys-body-medium-size));
  letter-spacing: var(--mat-checkbox-label-text-tracking, var(--mat-sys-body-medium-tracking));
  font-weight: var(--mat-checkbox-label-text-weight, var(--mat-sys-body-medium-weight));
}
.mat-mdc-checkbox.mat-mdc-checkbox-disabled.mat-mdc-checkbox-disabled-interactive {
  pointer-events: auto;
}
.mat-mdc-checkbox.mat-mdc-checkbox-disabled.mat-mdc-checkbox-disabled-interactive input {
  cursor: default;
}
.mat-mdc-checkbox.mat-mdc-checkbox-disabled label {
  cursor: default;
  color: var(--mat-checkbox-disabled-label-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
}
@media (forced-colors: active) {
  .mat-mdc-checkbox.mat-mdc-checkbox-disabled label {
    color: GrayText;
  }
}
.mat-mdc-checkbox label:empty {
  display: none;
}
.mat-mdc-checkbox .mdc-checkbox__ripple {
  opacity: 0;
}

.mat-mdc-checkbox .mat-mdc-checkbox-ripple,
.mdc-checkbox__ripple {
  top: 0;
  left: 0;
  right: 0;
  bottom: 0;
  position: absolute;
  border-radius: 50%;
  pointer-events: none;
}
.mat-mdc-checkbox .mat-mdc-checkbox-ripple:not(:empty),
.mdc-checkbox__ripple:not(:empty) {
  transform: translateZ(0);
}

.mat-mdc-checkbox-ripple .mat-ripple-element {
  opacity: 0.1;
}

.mat-mdc-checkbox-touch-target {
  position: absolute;
  top: 50%;
  left: 50%;
  height: var(--mat-checkbox-touch-target-size, 48px);
  width: var(--mat-checkbox-touch-target-size, 48px);
  transform: translate(-50%, -50%);
  display: var(--mat-checkbox-touch-target-display, block);
}

.mat-mdc-checkbox .mat-mdc-checkbox-ripple::before {
  border-radius: 50%;
}

.mdc-checkbox__native-control:focus-visible ~ .mat-focus-indicator::before {
  content: "";
}
`],encapsulation:2,changeDetection:0})}return t})(),Ik=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=ae({type:t});static \u0275inj=oe({imports:[Sy,Yt]})}return t})();var Hh=class t{baseUrl=Mr.apiUrl;http=d(Wt);deliveryMethods=M([]);getDeliveryMethods(){return this.deliveryMethods().length>0?W(this.deliveryMethods()):this.http.get(this.baseUrl+"payments/delivery-methods").pipe(Q(n=>(this.deliveryMethods.set(n.sort((e,i)=>i.price-e.price)),this.deliveryMethods())))}static \u0275fac=function(e){return new(e||t)};static \u0275prov=C({token:t,factory:t.\u0275fac,providedIn:"root"})};var pH=["input"],gH=["formField"],_H=["*"],Uh=class{source;value;constructor(n,e){this.source=n,this.value=e}},vH={provide:Ei,useExisting:It(()=>Iy),multi:!0},kk=new y("MatRadioGroup"),bH=new y("mat-radio-default-options",{providedIn:"root",factory:()=>({color:"accent",disabledInteractive:!1})}),Iy=(()=>{class t{_changeDetector=d(ve);_value=null;_name=d(Ie).getId("mat-radio-group-");_selected=null;_isInitialized=!1;_labelPosition="after";_disabled=!1;_required=!1;_buttonChanges;_controlValueAccessorChangeFn=()=>{};onTouched=()=>{};change=new U;_radios;color;get name(){return this._name}set name(e){this._name=e,this._updateRadioButtonNames()}get labelPosition(){return this._labelPosition}set labelPosition(e){this._labelPosition=e==="before"?"before":"after",this._markRadiosForCheck()}get value(){return this._value}set value(e){this._value!==e&&(this._value=e,this._updateSelectedRadioFromValue(),this._checkSelectedRadioButton())}_checkSelectedRadioButton(){this._selected&&!this._selected.checked&&(this._selected.checked=!0)}get selected(){return this._selected}set selected(e){this._selected=e,this.value=e?e.value:null,this._checkSelectedRadioButton()}get disabled(){return this._disabled}set disabled(e){this._disabled=e,this._markRadiosForCheck()}get required(){return this._required}set required(e){this._required=e,this._markRadiosForCheck()}get disabledInteractive(){return this._disabledInteractive}set disabledInteractive(e){this._disabledInteractive=e,this._markRadiosForCheck()}_disabledInteractive=!1;constructor(){}ngAfterContentInit(){this._isInitialized=!0,this._buttonChanges=this._radios.changes.subscribe(()=>{this.selected&&!this._radios.find(e=>e===this.selected)&&(this._selected=null)})}ngOnDestroy(){this._buttonChanges?.unsubscribe()}_touch(){this.onTouched&&this.onTouched()}_updateRadioButtonNames(){this._radios&&this._radios.forEach(e=>{e.name=this.name,e._markForCheck()})}_updateSelectedRadioFromValue(){let e=this._selected!==null&&this._selected.value===this._value;this._radios&&!e&&(this._selected=null,this._radios.forEach(i=>{i.checked=this.value===i.value,i.checked&&(this._selected=i)}))}_emitChangeEvent(){this._isInitialized&&this.change.emit(new Uh(this._selected,this._value))}_markRadiosForCheck(){this._radios&&this._radios.forEach(e=>e._markForCheck())}writeValue(e){this.value=e,this._changeDetector.markForCheck()}registerOnChange(e){this._controlValueAccessorChangeFn=e}registerOnTouched(e){this.onTouched=e}setDisabledState(e){this.disabled=e,this._changeDetector.markForCheck()}static \u0275fac=function(i){return new(i||t)};static \u0275dir=A({type:t,selectors:[["mat-radio-group"]],contentQueries:function(i,r,o){if(i&1&&yt(o,zh,5),i&2){let s;$(s=G())&&(r._radios=s)}},hostAttrs:["role","radiogroup",1,"mat-mdc-radio-group"],inputs:{color:"color",name:"name",labelPosition:"labelPosition",value:"value",selected:"selected",disabled:[2,"disabled","disabled",B],required:[2,"required","required",B],disabledInteractive:[2,"disabledInteractive","disabledInteractive",B]},outputs:{change:"change"},exportAs:["matRadioGroup"],features:[Ee([vH,{provide:kk,useExisting:t}])]})}return t})(),zh=(()=>{class t{_elementRef=d(L);_changeDetector=d(ve);_focusMonitor=d(Tn);_radioDispatcher=d(Bb);_defaultOptions=d(bH,{optional:!0});_ngZone=d(H);_renderer=d(xe);_uniqueId=d(Ie).getId("mat-radio-");_cleanupClick;id=this._uniqueId;name;ariaLabel;ariaLabelledby;ariaDescribedby;disableRipple=!1;tabIndex=0;get checked(){return this._checked}set checked(e){this._checked!==e&&(this._checked=e,e&&this.radioGroup&&this.radioGroup.value!==this.value?this.radioGroup.selected=this:!e&&this.radioGroup&&this.radioGroup.value===this.value&&(this.radioGroup.selected=null),e&&this._radioDispatcher.notify(this.id,this.name),this._changeDetector.markForCheck())}get value(){return this._value}set value(e){this._value!==e&&(this._value=e,this.radioGroup!==null&&(this.checked||(this.checked=this.radioGroup.value===e),this.checked&&(this.radioGroup.selected=this)))}get labelPosition(){return this._labelPosition||this.radioGroup&&this.radioGroup.labelPosition||"after"}set labelPosition(e){this._labelPosition=e}_labelPosition;get disabled(){return this._disabled||this.radioGroup!==null&&this.radioGroup.disabled}set disabled(e){this._setDisabled(e)}get required(){return this._required||this.radioGroup&&this.radioGroup.required}set required(e){e!==this._required&&this._changeDetector.markForCheck(),this._required=e}get color(){return this._color||this.radioGroup&&this.radioGroup.color||this._defaultOptions&&this._defaultOptions.color||"accent"}set color(e){this._color=e}_color;get disabledInteractive(){return this._disabledInteractive||this.radioGroup!==null&&this.radioGroup.disabledInteractive}set disabledInteractive(e){this._disabledInteractive=e}_disabledInteractive;change=new U;radioGroup;get inputId(){return`${this.id||this._uniqueId}-input`}_checked=!1;_disabled=!1;_required=!1;_value=null;_removeUniqueSelectionListener=()=>{};_previousTabIndex;_inputElement;_rippleTrigger;_noopAnimations=ke();_injector=d(X);constructor(){d(qe).load(An);let e=d(kk,{optional:!0}),i=d(new Kn("tabindex"),{optional:!0});this.radioGroup=e,this._disabledInteractive=this._defaultOptions?.disabledInteractive??!1,i&&(this.tabIndex=lt(i,0))}focus(e,i){i?this._focusMonitor.focusVia(this._inputElement,i,e):this._inputElement.nativeElement.focus(e)}_markForCheck(){this._changeDetector.markForCheck()}ngOnInit(){this.radioGroup&&(this.checked=this.radioGroup.value===this._value,this.checked&&(this.radioGroup.selected=this),this.name=this.radioGroup.name),this._removeUniqueSelectionListener=this._radioDispatcher.listen((e,i)=>{e!==this.id&&i===this.name&&(this.checked=!1)})}ngDoCheck(){this._updateTabIndex()}ngAfterViewInit(){this._updateTabIndex(),this._focusMonitor.monitor(this._elementRef,!0).subscribe(e=>{!e&&this.radioGroup&&this.radioGroup._touch()}),this._ngZone.runOutsideAngular(()=>{this._cleanupClick=this._renderer.listen(this._inputElement.nativeElement,"click",this._onInputClick)})}ngOnDestroy(){this._cleanupClick?.(),this._focusMonitor.stopMonitoring(this._elementRef),this._removeUniqueSelectionListener()}_emitChangeEvent(){this.change.emit(new Uh(this,this._value))}_isRippleDisabled(){return this.disableRipple||this.disabled}_onInputInteraction(e){if(e.stopPropagation(),!this.checked&&!this.disabled){let i=this.radioGroup&&this.value!==this.radioGroup.value;this.checked=!0,this._emitChangeEvent(),this.radioGroup&&(this.radioGroup._controlValueAccessorChangeFn(this.value),i&&this.radioGroup._emitChangeEvent())}}_onTouchTargetClick(e){this._onInputInteraction(e),(!this.disabled||this.disabledInteractive)&&this._inputElement?.nativeElement.focus()}_setDisabled(e){this._disabled!==e&&(this._disabled=e,this._changeDetector.markForCheck())}_onInputClick=e=>{this.disabled&&this.disabledInteractive&&e.preventDefault()};_updateTabIndex(){let e=this.radioGroup,i;if(!e||!e.selected||this.disabled?i=this.tabIndex:i=e.selected===this?this.tabIndex:-1,i!==this._previousTabIndex){let r=this._inputElement?.nativeElement;r&&(r.setAttribute("tabindex",i+""),this._previousTabIndex=i,bt(()=>{queueMicrotask(()=>{e&&e.selected&&e.selected!==this&&document.activeElement===r&&(e.selected?._inputElement.nativeElement.focus(),document.activeElement===r&&this._inputElement.nativeElement.blur())})},{injector:this._injector}))}}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=E({type:t,selectors:[["mat-radio-button"]],viewQuery:function(i,r){if(i&1&&Ve(pH,5)(gH,7,L),i&2){let o;$(o=G())&&(r._inputElement=o.first),$(o=G())&&(r._rippleTrigger=o.first)}},hostAttrs:[1,"mat-mdc-radio-button"],hostVars:19,hostBindings:function(i,r){i&1&&T("focus",function(){return r._inputElement.nativeElement.focus()}),i&2&&(q("id",r.id)("tabindex",null)("aria-label",null)("aria-labelledby",null)("aria-describedby",null),z("mat-primary",r.color==="primary")("mat-accent",r.color==="accent")("mat-warn",r.color==="warn")("mat-mdc-radio-checked",r.checked)("mat-mdc-radio-disabled",r.disabled)("mat-mdc-radio-disabled-interactive",r.disabledInteractive)("_mat-animation-noopable",r._noopAnimations))},inputs:{id:"id",name:"name",ariaLabel:[0,"aria-label","ariaLabel"],ariaLabelledby:[0,"aria-labelledby","ariaLabelledby"],ariaDescribedby:[0,"aria-describedby","ariaDescribedby"],disableRipple:[2,"disableRipple","disableRipple",B],tabIndex:[2,"tabIndex","tabIndex",e=>e==null?0:lt(e)],checked:[2,"checked","checked",B],value:"value",labelPosition:"labelPosition",disabled:[2,"disabled","disabled",B],required:[2,"required","required",B],color:"color",disabledInteractive:[2,"disabledInteractive","disabledInteractive",B]},outputs:{change:"change"},exportAs:["matRadioButton"],ngContentSelectors:_H,decls:13,vars:17,consts:[["formField",""],["input",""],["mat-internal-form-field","",3,"labelPosition"],[1,"mdc-radio"],["aria-hidden","true",1,"mat-mdc-radio-touch-target",3,"click"],["type","radio","aria-invalid","false",1,"mdc-radio__native-control",3,"change","id","checked","disabled","required"],["aria-hidden","true",1,"mdc-radio__background"],[1,"mdc-radio__outer-circle"],[1,"mdc-radio__inner-circle"],["mat-ripple","","aria-hidden","true",1,"mat-radio-ripple","mat-focus-indicator",3,"matRippleTrigger","matRippleDisabled","matRippleCentered"],[1,"mat-ripple-element","mat-radio-persistent-ripple"],[1,"mdc-label",3,"for"]],template:function(i,r){i&1&&(Ce(),f(0,"div",2,0)(2,"div",3)(3,"div",4),T("click",function(s){return r._onTouchTargetClick(s)}),h(),f(4,"input",5,1),T("change",function(s){return r._onInputInteraction(s)}),h(),f(6,"div",6),O(7,"div",7)(8,"div",8),h(),f(9,"div",9),O(10,"div",10),h()(),f(11,"label",11),re(12),h()()),i&2&&(D("labelPosition",r.labelPosition),p(2),z("mdc-radio--disabled",r.disabled),p(2),D("id",r.inputId)("checked",r.checked)("disabled",r.disabled&&!r.disabledInteractive)("required",r.required),q("name",r.name)("value",r.value)("aria-label",r.ariaLabel)("aria-labelledby",r.ariaLabelledby)("aria-describedby",r.ariaDescribedby)("aria-disabled",r.disabled&&r.disabledInteractive?"true":null),p(5),D("matRippleTrigger",r._rippleTrigger.nativeElement)("matRippleDisabled",r._isRippleDisabled())("matRippleCentered",!0),p(2),D("for",r.inputId))},dependencies:[Xi,Vh],styles:[`.mat-mdc-radio-button {
  -webkit-tap-highlight-color: transparent;
}
.mat-mdc-radio-button .mdc-radio {
  display: inline-block;
  position: relative;
  flex: 0 0 auto;
  box-sizing: content-box;
  width: 20px;
  height: 20px;
  cursor: pointer;
  will-change: opacity, transform, border-color, color;
  padding: calc((var(--mat-radio-state-layer-size, 40px) - 20px) / 2);
}
.mat-mdc-radio-button .mdc-radio:hover > .mdc-radio__native-control:not([disabled]):not(:focus) ~ .mdc-radio__background::before {
  opacity: 0.04;
  transform: scale(1);
}
.mat-mdc-radio-button .mdc-radio:hover > .mdc-radio__native-control:not([disabled]) ~ .mdc-radio__background > .mdc-radio__outer-circle {
  border-color: var(--mat-radio-unselected-hover-icon-color, var(--mat-sys-on-surface));
}
.mat-mdc-radio-button .mdc-radio:hover > .mdc-radio__native-control:enabled:checked + .mdc-radio__background > .mdc-radio__outer-circle {
  border-color: var(--mat-radio-selected-hover-icon-color, var(--mat-sys-primary));
}
.mat-mdc-radio-button .mdc-radio:hover > .mdc-radio__native-control:enabled:checked + .mdc-radio__background > .mdc-radio__inner-circle {
  background-color: var(--mat-radio-selected-hover-icon-color, var(--mat-sys-primary, currentColor));
}
.mat-mdc-radio-button .mdc-radio:active > .mdc-radio__native-control:enabled:not(:checked) + .mdc-radio__background > .mdc-radio__outer-circle {
  border-color: var(--mat-radio-unselected-pressed-icon-color, var(--mat-sys-on-surface));
}
.mat-mdc-radio-button .mdc-radio:active > .mdc-radio__native-control:enabled:checked + .mdc-radio__background > .mdc-radio__outer-circle {
  border-color: var(--mat-radio-selected-pressed-icon-color, var(--mat-sys-primary));
}
.mat-mdc-radio-button .mdc-radio:active > .mdc-radio__native-control:enabled:checked + .mdc-radio__background > .mdc-radio__inner-circle {
  background-color: var(--mat-radio-selected-pressed-icon-color, var(--mat-sys-primary, currentColor));
}
.mat-mdc-radio-button .mdc-radio__background {
  display: inline-block;
  position: relative;
  box-sizing: border-box;
  width: 20px;
  height: 20px;
}
.mat-mdc-radio-button .mdc-radio__background::before {
  position: absolute;
  transform: scale(0, 0);
  border-radius: 50%;
  opacity: 0;
  pointer-events: none;
  content: "";
  transition: opacity 90ms cubic-bezier(0.4, 0, 0.6, 1), transform 90ms cubic-bezier(0.4, 0, 0.6, 1);
  width: var(--mat-radio-state-layer-size, 40px);
  height: var(--mat-radio-state-layer-size, 40px);
  top: calc(-1 * (var(--mat-radio-state-layer-size, 40px) - 20px) / 2);
  left: calc(-1 * (var(--mat-radio-state-layer-size, 40px) - 20px) / 2);
}
.mat-mdc-radio-button .mdc-radio__outer-circle {
  position: absolute;
  top: 0;
  left: 0;
  box-sizing: border-box;
  width: 100%;
  height: 100%;
  border-width: 2px;
  border-style: solid;
  border-radius: 50%;
  transition: border-color 90ms cubic-bezier(0.4, 0, 0.6, 1);
}
.mat-mdc-radio-button .mdc-radio__inner-circle {
  position: absolute;
  top: 0;
  left: 0;
  box-sizing: border-box;
  width: 100%;
  height: 100%;
  transform: scale(0);
  border-radius: 50%;
  transition: transform 90ms cubic-bezier(0.4, 0, 0.6, 1), background-color 90ms cubic-bezier(0.4, 0, 0.6, 1);
}
@media (forced-colors: active) {
  .mat-mdc-radio-button .mdc-radio__inner-circle {
    background-color: CanvasText !important;
  }
}
.mat-mdc-radio-button .mdc-radio__native-control {
  position: absolute;
  margin: 0;
  padding: 0;
  opacity: 0;
  top: 0;
  right: 0;
  left: 0;
  cursor: inherit;
  z-index: 1;
  width: var(--mat-radio-state-layer-size, 40px);
  height: var(--mat-radio-state-layer-size, 40px);
}
.mat-mdc-radio-button .mdc-radio__native-control:checked + .mdc-radio__background, .mat-mdc-radio-button .mdc-radio__native-control:disabled + .mdc-radio__background {
  transition: opacity 90ms cubic-bezier(0, 0, 0.2, 1), transform 90ms cubic-bezier(0, 0, 0.2, 1);
}
.mat-mdc-radio-button .mdc-radio__native-control:checked + .mdc-radio__background > .mdc-radio__outer-circle, .mat-mdc-radio-button .mdc-radio__native-control:disabled + .mdc-radio__background > .mdc-radio__outer-circle {
  transition: border-color 90ms cubic-bezier(0, 0, 0.2, 1);
}
.mat-mdc-radio-button .mdc-radio__native-control:checked + .mdc-radio__background > .mdc-radio__inner-circle, .mat-mdc-radio-button .mdc-radio__native-control:disabled + .mdc-radio__background > .mdc-radio__inner-circle {
  transition: transform 90ms cubic-bezier(0, 0, 0.2, 1), background-color 90ms cubic-bezier(0, 0, 0.2, 1);
}
.mat-mdc-radio-button .mdc-radio__native-control:focus + .mdc-radio__background::before {
  transform: scale(1);
  opacity: 0.12;
  transition: opacity 90ms cubic-bezier(0, 0, 0.2, 1), transform 90ms cubic-bezier(0, 0, 0.2, 1);
}
.mat-mdc-radio-button .mdc-radio__native-control:disabled:not(:checked) + .mdc-radio__background > .mdc-radio__outer-circle {
  border-color: var(--mat-radio-disabled-unselected-icon-color, var(--mat-sys-on-surface));
  opacity: var(--mat-radio-disabled-unselected-icon-opacity, 0.38);
}
.mat-mdc-radio-button .mdc-radio__native-control:disabled + .mdc-radio__background {
  cursor: default;
}
.mat-mdc-radio-button .mdc-radio__native-control:disabled + .mdc-radio__background > .mdc-radio__outer-circle {
  border-color: var(--mat-radio-disabled-selected-icon-color, var(--mat-sys-on-surface));
  opacity: var(--mat-radio-disabled-selected-icon-opacity, 0.38);
}
.mat-mdc-radio-button .mdc-radio__native-control:disabled + .mdc-radio__background > .mdc-radio__inner-circle {
  background-color: var(--mat-radio-disabled-selected-icon-color, var(--mat-sys-on-surface, currentColor));
  opacity: var(--mat-radio-disabled-selected-icon-opacity, 0.38);
}
.mat-mdc-radio-button .mdc-radio__native-control:enabled:not(:checked) + .mdc-radio__background > .mdc-radio__outer-circle {
  border-color: var(--mat-radio-unselected-icon-color, var(--mat-sys-on-surface-variant));
}
.mat-mdc-radio-button .mdc-radio__native-control:enabled:checked + .mdc-radio__background > .mdc-radio__outer-circle {
  border-color: var(--mat-radio-selected-icon-color, var(--mat-sys-primary));
}
.mat-mdc-radio-button .mdc-radio__native-control:enabled:checked + .mdc-radio__background > .mdc-radio__inner-circle {
  background-color: var(--mat-radio-selected-icon-color, var(--mat-sys-primary, currentColor));
}
.mat-mdc-radio-button .mdc-radio__native-control:enabled:focus:checked + .mdc-radio__background > .mdc-radio__outer-circle {
  border-color: var(--mat-radio-selected-focus-icon-color, var(--mat-sys-primary));
}
.mat-mdc-radio-button .mdc-radio__native-control:enabled:focus:checked + .mdc-radio__background > .mdc-radio__inner-circle {
  background-color: var(--mat-radio-selected-focus-icon-color, var(--mat-sys-primary, currentColor));
}
.mat-mdc-radio-button .mdc-radio__native-control:checked + .mdc-radio__background > .mdc-radio__inner-circle {
  transform: scale(0.5);
  transition: transform 90ms cubic-bezier(0, 0, 0.2, 1), background-color 90ms cubic-bezier(0, 0, 0.2, 1);
}
.mat-mdc-radio-button.mat-mdc-radio-disabled-interactive .mdc-radio--disabled {
  pointer-events: auto;
}
.mat-mdc-radio-button.mat-mdc-radio-disabled-interactive .mdc-radio--disabled .mdc-radio__native-control:not(:checked) + .mdc-radio__background > .mdc-radio__outer-circle {
  border-color: var(--mat-radio-disabled-unselected-icon-color, var(--mat-sys-on-surface));
  opacity: var(--mat-radio-disabled-unselected-icon-opacity, 0.38);
}
.mat-mdc-radio-button.mat-mdc-radio-disabled-interactive .mdc-radio--disabled:hover .mdc-radio__native-control:checked + .mdc-radio__background > .mdc-radio__outer-circle,
.mat-mdc-radio-button.mat-mdc-radio-disabled-interactive .mdc-radio--disabled .mdc-radio__native-control:checked:focus + .mdc-radio__background > .mdc-radio__outer-circle,
.mat-mdc-radio-button.mat-mdc-radio-disabled-interactive .mdc-radio--disabled .mdc-radio__native-control + .mdc-radio__background > .mdc-radio__outer-circle {
  border-color: var(--mat-radio-disabled-selected-icon-color, var(--mat-sys-on-surface));
  opacity: var(--mat-radio-disabled-selected-icon-opacity, 0.38);
}
.mat-mdc-radio-button.mat-mdc-radio-disabled-interactive .mdc-radio--disabled:hover .mdc-radio__native-control:checked + .mdc-radio__background > .mdc-radio__inner-circle,
.mat-mdc-radio-button.mat-mdc-radio-disabled-interactive .mdc-radio--disabled .mdc-radio__native-control:checked:focus + .mdc-radio__background > .mdc-radio__inner-circle,
.mat-mdc-radio-button.mat-mdc-radio-disabled-interactive .mdc-radio--disabled .mdc-radio__native-control + .mdc-radio__background > .mdc-radio__inner-circle {
  background-color: var(--mat-radio-disabled-selected-icon-color, var(--mat-sys-on-surface, currentColor));
  opacity: var(--mat-radio-disabled-selected-icon-opacity, 0.38);
}
.mat-mdc-radio-button._mat-animation-noopable .mdc-radio__background::before,
.mat-mdc-radio-button._mat-animation-noopable .mdc-radio__outer-circle,
.mat-mdc-radio-button._mat-animation-noopable .mdc-radio__inner-circle {
  transition: none !important;
}
.mat-mdc-radio-button label {
  cursor: pointer;
}
.mat-mdc-radio-button label:empty {
  display: none;
}
.mat-mdc-radio-button .mdc-radio__background::before {
  background-color: var(--mat-radio-ripple-color, var(--mat-sys-on-surface));
}
.mat-mdc-radio-button.mat-mdc-radio-checked .mat-ripple-element,
.mat-mdc-radio-button.mat-mdc-radio-checked .mdc-radio__background::before {
  background-color: var(--mat-radio-checked-ripple-color, var(--mat-sys-primary));
}
.mat-mdc-radio-button.mat-mdc-radio-disabled-interactive .mdc-radio--disabled .mat-ripple-element,
.mat-mdc-radio-button.mat-mdc-radio-disabled-interactive .mdc-radio--disabled .mdc-radio__background::before {
  background-color: var(--mat-radio-ripple-color, var(--mat-sys-on-surface));
}
.mat-mdc-radio-button .mat-internal-form-field {
  color: var(--mat-radio-label-text-color, var(--mat-sys-on-surface));
  font-family: var(--mat-radio-label-text-font, var(--mat-sys-body-medium-font));
  line-height: var(--mat-radio-label-text-line-height, var(--mat-sys-body-medium-line-height));
  font-size: var(--mat-radio-label-text-size, var(--mat-sys-body-medium-size));
  letter-spacing: var(--mat-radio-label-text-tracking, var(--mat-sys-body-medium-tracking));
  font-weight: var(--mat-radio-label-text-weight, var(--mat-sys-body-medium-weight));
}
.mat-mdc-radio-button .mdc-radio--disabled + label {
  color: var(--mat-radio-disabled-label-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
}
.mat-mdc-radio-button .mat-radio-ripple {
  top: 0;
  left: 0;
  right: 0;
  bottom: 0;
  position: absolute;
  pointer-events: none;
  border-radius: 50%;
}
.mat-mdc-radio-button .mat-radio-ripple > .mat-ripple-element {
  opacity: 0.14;
}
.mat-mdc-radio-button .mat-radio-ripple::before {
  border-radius: 50%;
}
.mat-mdc-radio-button .mdc-radio > .mdc-radio__native-control:focus:enabled:not(:checked) ~ .mdc-radio__background > .mdc-radio__outer-circle {
  border-color: var(--mat-radio-unselected-focus-icon-color, var(--mat-sys-on-surface));
}
.mat-mdc-radio-button.cdk-focused .mat-focus-indicator::before {
  content: "";
}

.mat-mdc-radio-disabled {
  cursor: default;
  pointer-events: none;
}
.mat-mdc-radio-disabled.mat-mdc-radio-disabled-interactive {
  pointer-events: auto;
}

.mat-mdc-radio-touch-target {
  position: absolute;
  top: 50%;
  left: 50%;
  height: var(--mat-radio-touch-target-size, 48px);
  width: var(--mat-radio-touch-target-size, 48px);
  transform: translate(-50%, -50%);
  display: var(--mat-radio-touch-target-display, block);
}
[dir=rtl] .mat-mdc-radio-touch-target {
  left: auto;
  right: 50%;
  transform: translate(50%, -50%);
}
`],encapsulation:2,changeDetection:0})}return t})(),Mk=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=ae({type:t});static \u0275inj=oe({imports:[kf,zh,Yt]})}return t})();var xH=(t,n)=>n.id;function CH(t,n){if(t&1&&(f(0,"label",2)(1,"mat-radio-button",3)(2,"div",4)(3,"strong"),_(4),Se(5,"currency"),h(),f(6,"span",5),_(7),h()()()()),t&2){let e=n.$implicit;p(),D("value",e),p(3),ia("",e.shortName," - ",Oe(5,4,e.price)),p(3),V(e.description)}}var $h=class t{checkoutService=d(Hh);cartService=d(rt);deliveryComplete=nD();ngOnInit(){this.checkoutService.getDeliveryMethods().subscribe({next:n=>{if(this.cartService.cart()?.deliveryMethodId){let e=n.find(i=>i.id===this.cartService.cart()?.deliveryMethodId);e&&(this.cartService.selectedDeliveryMethod.set(e),this.deliveryComplete.emit(!0))}}})}updateSelectedDeliveryMethod(n){this.cartService.selectedDeliveryMethod.set(n);let e=this.cartService.cart();e&&(e.deliveryMethodId=n.id,this.cartService.setCart(e),this.deliveryComplete.emit(!0))}static \u0275fac=function(e){return new(e||t)};static \u0275cmp=E({type:t,selectors:[["app-checkout-delivery"]],outputs:{deliveryComplete:"deliveryComplete"},decls:4,vars:1,consts:[[1,"w-full"],[1,"grid","grid-cols-1","gap-4",3,"change","value"],[1,"p-3","border","border-gray-200","rounded-lg","cursor-pointer","w-full","hover:bg-blue-100"],[1,"w-full",3,"value"],[1,"flex","flex-col"],[1,"text-sm","text-gray-500"]],template:function(e,i){e&1&&(f(0,"div",0)(1,"mat-radio-group",1),T("change",function(o){return i.updateSelectedDeliveryMethod(o.value)}),tt(2,CH,8,6,"label",2,xH),h()()),e&2&&(p(),D("value",i.cartService.selectedDeliveryMethod()),p(),nt(i.checkoutService.deliveryMethods()))},dependencies:[Mk,Iy,zh,Ut],encapsulation:2})};var yo=class t{transform(n,...e){if(n&&"address"in n&&n.name){let{line1:i,line2:r,city:o,state:s,postal_code:a,country:c}=n?.address;return`${n.name}, ${i}${r?", "+r:""}, ${o}, ${s}, ${a}, ${c}`}else if(n&&"line1"in n){let{line1:i,line2:r,city:o,state:s,postalCode:a,country:c}=n;return`${n.name}, ${i}${r?", "+r:""}, ${o}, ${s}, ${a}, ${c}`}else return"Unknown Address"}static \u0275fac=function(e){return new(e||t)};static \u0275pipe=Qr({name:"address",type:t,pure:!0})};var xo=class t{transform(n,...e){if(n&&"card"in n){let{brand:i,last4:r,exp_month:o,exp_year:s}=n.card;return`${i.toUpperCase()} **** **** **** ${r}, EXP: ${o.toString().padStart(2,"0")}/${s}`}else if(n&&"brand"in n){let{brand:i,last4Digits:r,expMonth:o,expYear:s}=n;return`${i.toUpperCase()} **** **** **** ${r}, EXP: ${o.toString().padStart(2,"0")}/${s}`}else return"Unknown Payment Method"}static \u0275fac=function(e){return new(e||t)};static \u0275pipe=Qr({name:"paymentMethod",type:t,pure:!0})};var wH=(t,n)=>n.productId;function DH(t,n){if(t&1&&(Fe(0,"tr")(1,"td",8)(2,"div",9),Ht(3,"img",10),Fe(4,"span"),_(5),Ge()()(),Fe(6,"td",11),_(7),Ge(),Fe(8,"td",12),_(9),Se(10,"currency"),Ge()()),t&2){let e=n.$implicit;p(3),mt("src",an(e.pictureUrl),Qn),p(2),V(e.productName),p(2),we(" x",e.quantity," "),p(2),we(" ",Oe(10,5,e.price)," ")}}var Gh=class t{confirmationToken=null;cartService=d(rt);static \u0275fac=function(e){return new(e||t)};static \u0275cmp=E({type:t,selectors:[["app-checkout-review"]],inputs:{confirmationToken:"confirmationToken"},decls:20,vars:6,consts:[[1,"mt-2","w-full"],[1,"text-lg","font-semibold"],[1,"font-medium","mt-2"],[1,"mt-1","text-gray-500"],[1,"mt-4","mx-auto"],[1,"border-b","border-gray-200"],[1,"w-full"],[1,"divide-y","divide-gray-200"],[1,"py-4"],[1,"flex","items-center","gap-4"],["alt","product image",1,"w-12","h-12",3,"src"],[1,"py-4","text-center"],[1,"py-4","text-end"]],template:function(e,i){if(e&1&&(Fe(0,"div",0)(1,"h4",1),_(2,"Billing and delivery information"),Ge(),Fe(3,"dl")(4,"dt",2),_(5,"Shipping address"),Ge(),Fe(6,"dd",3),_(7),Se(8,"address"),Ge(),Fe(9,"dt",2),_(10,"Payment details"),Ge(),Fe(11,"dd",3),_(12),Se(13,"paymentMethod"),Ge()()(),Fe(14,"div",4)(15,"div",5)(16,"table",6)(17,"tbody",7),tt(18,DH,11,7,"tr",null,wH),Ge()()()()),e&2){let r;p(7),V(Oe(8,2,i.confirmationToken==null?null:i.confirmationToken.shipping)),p(5),V(Oe(13,4,i.confirmationToken==null?null:i.confirmationToken.payment_method_preview)),p(6),nt((r=i.cartService.cart())==null?null:r.items)}},dependencies:[Ut,yo,xo],encapsulation:2})};var EH=["determinateSpinner"];function SH(t,n){if(t&1&&(Gt(),f(0,"svg",11),O(1,"circle",12),h()),t&2){let e=S();q("viewBox",e._viewBox()),p(),In("stroke-dasharray",e._strokeCircumference(),"px")("stroke-dashoffset",e._strokeCircumference()/2,"px")("stroke-width",e._circleStrokeWidth(),"%"),q("r",e._circleRadius())}}var IH=new y("mat-progress-spinner-default-options",{providedIn:"root",factory:()=>({diameter:Tk})}),Tk=100,kH=10,Wh=(()=>{class t{_elementRef=d(L);_noopAnimations;get color(){return this._color||this._defaultColor}set color(e){this._color=e}_color;_defaultColor="primary";_determinateCircle;constructor(){let e=d(IH),i=Hl(),r=this._elementRef.nativeElement;this._noopAnimations=i==="di-disabled"&&!!e&&!e._forceAnimations,this.mode=r.nodeName.toLowerCase()==="mat-spinner"?"indeterminate":"determinate",!this._noopAnimations&&i==="reduced-motion"&&r.classList.add("mat-progress-spinner-reduced-motion"),e&&(e.color&&(this.color=this._defaultColor=e.color),e.diameter&&(this.diameter=e.diameter),e.strokeWidth&&(this.strokeWidth=e.strokeWidth))}mode;get value(){return this.mode==="determinate"?this._value:0}set value(e){this._value=Math.max(0,Math.min(100,e||0))}_value=0;get diameter(){return this._diameter}set diameter(e){this._diameter=e||0}_diameter=Tk;get strokeWidth(){return this._strokeWidth??this.diameter/10}set strokeWidth(e){this._strokeWidth=e||0}_strokeWidth;_circleRadius(){return(this.diameter-kH)/2}_viewBox(){let e=this._circleRadius()*2+this.strokeWidth;return`0 0 ${e} ${e}`}_strokeCircumference(){return 2*Math.PI*this._circleRadius()}_strokeDashOffset(){return this.mode==="determinate"?this._strokeCircumference()*(100-this._value)/100:null}_circleStrokeWidth(){return this.strokeWidth/this.diameter*100}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=E({type:t,selectors:[["mat-progress-spinner"],["mat-spinner"]],viewQuery:function(i,r){if(i&1&&Ve(EH,5),i&2){let o;$(o=G())&&(r._determinateCircle=o.first)}},hostAttrs:["role","progressbar","tabindex","-1",1,"mat-mdc-progress-spinner","mdc-circular-progress"],hostVars:18,hostBindings:function(i,r){i&2&&(q("aria-valuemin",0)("aria-valuemax",100)("aria-valuenow",r.mode==="determinate"?r.value:null)("mode",r.mode),Tt("mat-"+r.color),In("width",r.diameter,"px")("height",r.diameter,"px")("--mat-progress-spinner-size",r.diameter+"px")("--mat-progress-spinner-active-indicator-width",r.diameter+"px"),z("_mat-animation-noopable",r._noopAnimations)("mdc-circular-progress--indeterminate",r.mode==="indeterminate"))},inputs:{color:"color",mode:"mode",value:[2,"value","value",lt],diameter:[2,"diameter","diameter",lt],strokeWidth:[2,"strokeWidth","strokeWidth",lt]},exportAs:["matProgressSpinner"],decls:14,vars:11,consts:[["circle",""],["determinateSpinner",""],["aria-hidden","true",1,"mdc-circular-progress__determinate-container"],["xmlns","http://www.w3.org/2000/svg","focusable","false",1,"mdc-circular-progress__determinate-circle-graphic"],["cx","50%","cy","50%",1,"mdc-circular-progress__determinate-circle"],["aria-hidden","true",1,"mdc-circular-progress__indeterminate-container"],[1,"mdc-circular-progress__spinner-layer"],[1,"mdc-circular-progress__circle-clipper","mdc-circular-progress__circle-left"],[3,"ngTemplateOutlet"],[1,"mdc-circular-progress__gap-patch"],[1,"mdc-circular-progress__circle-clipper","mdc-circular-progress__circle-right"],["xmlns","http://www.w3.org/2000/svg","focusable","false",1,"mdc-circular-progress__indeterminate-circle-graphic"],["cx","50%","cy","50%"]],template:function(i,r){if(i&1&&($e(0,SH,2,8,"ng-template",null,0,Xn),f(2,"div",2,1),Gt(),f(4,"svg",3),O(5,"circle",4),h()(),Vi(),f(6,"div",5)(7,"div",6)(8,"div",7),sn(9,8),h(),f(10,"div",9),sn(11,8),h(),f(12,"div",10),sn(13,8),h()()()),i&2){let o=Re(1);p(4),q("viewBox",r._viewBox()),p(),In("stroke-dasharray",r._strokeCircumference(),"px")("stroke-dashoffset",r._strokeDashOffset(),"px")("stroke-width",r._circleStrokeWidth(),"%"),q("r",r._circleRadius()),p(4),D("ngTemplateOutlet",o),p(2),D("ngTemplateOutlet",o),p(2),D("ngTemplateOutlet",o)}},dependencies:[qi],styles:[`.mat-mdc-progress-spinner {
  --mat-progress-spinner-animation-multiplier: 1;
  display: block;
  overflow: hidden;
  line-height: 0;
  position: relative;
  direction: ltr;
  transition: opacity 250ms cubic-bezier(0.4, 0, 0.6, 1);
}
.mat-mdc-progress-spinner circle {
  stroke-width: var(--mat-progress-spinner-active-indicator-width, 4px);
}
.mat-mdc-progress-spinner._mat-animation-noopable, .mat-mdc-progress-spinner._mat-animation-noopable .mdc-circular-progress__determinate-circle {
  transition: none !important;
}
.mat-mdc-progress-spinner._mat-animation-noopable .mdc-circular-progress__indeterminate-circle-graphic,
.mat-mdc-progress-spinner._mat-animation-noopable .mdc-circular-progress__spinner-layer,
.mat-mdc-progress-spinner._mat-animation-noopable .mdc-circular-progress__indeterminate-container {
  animation: none !important;
}
.mat-mdc-progress-spinner._mat-animation-noopable .mdc-circular-progress__indeterminate-container circle {
  stroke-dasharray: 0 !important;
}
@media (forced-colors: active) {
  .mat-mdc-progress-spinner .mdc-circular-progress__indeterminate-circle-graphic,
  .mat-mdc-progress-spinner .mdc-circular-progress__determinate-circle {
    stroke: currentColor;
    stroke: CanvasText;
  }
}

.mat-progress-spinner-reduced-motion {
  --mat-progress-spinner-animation-multiplier: 1.25;
}

.mdc-circular-progress__determinate-container,
.mdc-circular-progress__indeterminate-circle-graphic,
.mdc-circular-progress__indeterminate-container,
.mdc-circular-progress__spinner-layer {
  position: absolute;
  width: 100%;
  height: 100%;
}

.mdc-circular-progress__determinate-container {
  transform: rotate(-90deg);
}
.mdc-circular-progress--indeterminate .mdc-circular-progress__determinate-container {
  opacity: 0;
}

.mdc-circular-progress__indeterminate-container {
  font-size: 0;
  letter-spacing: 0;
  white-space: nowrap;
  opacity: 0;
}
.mdc-circular-progress--indeterminate .mdc-circular-progress__indeterminate-container {
  opacity: 1;
  animation: mdc-circular-progress-container-rotate calc(1568.2352941176ms * var(--mat-progress-spinner-animation-multiplier)) linear infinite;
}

.mdc-circular-progress__determinate-circle-graphic,
.mdc-circular-progress__indeterminate-circle-graphic {
  fill: transparent;
}

.mat-mdc-progress-spinner .mdc-circular-progress__determinate-circle,
.mat-mdc-progress-spinner .mdc-circular-progress__indeterminate-circle-graphic {
  stroke: var(--mat-progress-spinner-active-indicator-color, var(--mat-sys-primary));
}
@media (forced-colors: active) {
  .mat-mdc-progress-spinner .mdc-circular-progress__determinate-circle,
  .mat-mdc-progress-spinner .mdc-circular-progress__indeterminate-circle-graphic {
    stroke: CanvasText;
  }
}

.mdc-circular-progress__determinate-circle {
  transition: stroke-dashoffset 500ms cubic-bezier(0, 0, 0.2, 1);
}

.mdc-circular-progress__gap-patch {
  position: absolute;
  top: 0;
  left: 47.5%;
  box-sizing: border-box;
  width: 5%;
  height: 100%;
  overflow: hidden;
}

.mdc-circular-progress__gap-patch .mdc-circular-progress__indeterminate-circle-graphic {
  left: -900%;
  width: 2000%;
  transform: rotate(180deg);
}
.mdc-circular-progress__circle-clipper .mdc-circular-progress__indeterminate-circle-graphic {
  width: 200%;
}
.mdc-circular-progress__circle-right .mdc-circular-progress__indeterminate-circle-graphic {
  left: -100%;
}
.mdc-circular-progress--indeterminate .mdc-circular-progress__circle-left .mdc-circular-progress__indeterminate-circle-graphic {
  animation: mdc-circular-progress-left-spin calc(1333ms * var(--mat-progress-spinner-animation-multiplier)) cubic-bezier(0.4, 0, 0.2, 1) infinite both;
}
.mdc-circular-progress--indeterminate .mdc-circular-progress__circle-right .mdc-circular-progress__indeterminate-circle-graphic {
  animation: mdc-circular-progress-right-spin calc(1333ms * var(--mat-progress-spinner-animation-multiplier)) cubic-bezier(0.4, 0, 0.2, 1) infinite both;
}

.mdc-circular-progress__circle-clipper {
  display: inline-flex;
  position: relative;
  width: 50%;
  height: 100%;
  overflow: hidden;
}

.mdc-circular-progress--indeterminate .mdc-circular-progress__spinner-layer {
  animation: mdc-circular-progress-spinner-layer-rotate calc(5332ms * var(--mat-progress-spinner-animation-multiplier)) cubic-bezier(0.4, 0, 0.2, 1) infinite both;
}

@keyframes mdc-circular-progress-container-rotate {
  to {
    transform: rotate(360deg);
  }
}
@keyframes mdc-circular-progress-spinner-layer-rotate {
  12.5% {
    transform: rotate(135deg);
  }
  25% {
    transform: rotate(270deg);
  }
  37.5% {
    transform: rotate(405deg);
  }
  50% {
    transform: rotate(540deg);
  }
  62.5% {
    transform: rotate(675deg);
  }
  75% {
    transform: rotate(810deg);
  }
  87.5% {
    transform: rotate(945deg);
  }
  100% {
    transform: rotate(1080deg);
  }
}
@keyframes mdc-circular-progress-left-spin {
  from {
    transform: rotate(265deg);
  }
  50% {
    transform: rotate(130deg);
  }
  to {
    transform: rotate(265deg);
  }
}
@keyframes mdc-circular-progress-right-spin {
  from {
    transform: rotate(-265deg);
  }
  50% {
    transform: rotate(-130deg);
  }
  to {
    transform: rotate(-265deg);
  }
}
`],encapsulation:2,changeDetection:0})}return t})();var qh=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=ae({type:t});static \u0275inj=oe({imports:[Yt]})}return t})();var ri=class t{baseUrl=Mr.apiUrl;http=d(Wt);orderCompleted=M(!1);createOrder(n){return this.http.post(this.baseUrl+"orders",n)}getOrdersForUser(){return this.http.get(this.baseUrl+"orders")}getOrderDetails(n){return this.http.get(this.baseUrl+"orders/"+n)}static \u0275fac=function(e){return new(e||t)};static \u0275prov=C({token:t,factory:t.\u0275fac,providedIn:"root"})};function MH(t,n){t&1&&O(0,"mat-spinner",22)}function TH(t,n){t&1&&(f(0,"span"),_(1,"Confirm Payment"),h())}var Yh=class t{router=d(Xe);stripeService=d(Bh);accountService=d(dn);cartService=d(rt);orderService=d(ri);snackbar=d(Rn);addressElement;paymentElement;saveAddress=!1;completionStatus=M({address:!1,delivery:!1,card:!1});confirmationToken=M(null);confirmPaymentLoading=M(!1);async ngOnInit(){try{this.addressElement=await this.stripeService.createAddressElement(),this.addressElement.mount("#address-element"),this.addressElement.on("change",n=>{this.completionStatus.update(e=>Z(b({},e),{address:n.complete}))}),this.paymentElement=await this.stripeService.createPaymentElement(),this.paymentElement.mount("#payment-element"),this.paymentElement.on("change",n=>{this.completionStatus.update(e=>Z(b({},e),{card:n.complete}))})}catch(n){this.snackbar.error(n.message)}}ngOnDestroy(){this.stripeService.disposeElements()}onSaveAddressCheckboxChange(n){this.saveAddress=n.checked}handleDeliveryChange(n){this.completionStatus.update(e=>Z(b({},e),{delivery:n}))}async getConfirmationToken(){try{if(Object.values(this.completionStatus()).every(n=>n===!0)){let n=await this.stripeService.createConfirmationToken();if(n.error)throw new Error(n.error.message);this.confirmationToken.set(n.confirmationToken)}}catch(n){this.snackbar.error(n.message)}}async onStepChange(n){if(n.selectedIndex===1&&this.saveAddress){let e=await this.getAdreessFromStripeElement();e&&Ro(this.accountService.updateAddress(e))}n.selectedIndex===2&&await Ro(this.stripeService.createOrUpdatePaymentIntent()),n.selectedIndex===3&&await this.getConfirmationToken()}async confirmPayment(n){this.confirmPaymentLoading.set(!0);try{let e=this.confirmationToken();if(!e)throw new Error("Confirmation token not found");let i=await this.stripeService.confirmPayment(e);if(i.paymentIntent?.status==="succeeded"){let r=await this.createOrderModel();if(await Ro(this.orderService.createOrder(r)))this.orderService.orderCompleted.set(!0),this.cartService.deleteCart(),this.cartService.selectedDeliveryMethod.set(null),this.router.navigateByUrl("/checkout/success");else throw new Error("Order creation failed")}else throw i.error?new Error(i.error.message):new Error("Something went wrong during payment confirmation")}catch(e){this.snackbar.error(e.message||"Payment failed"),n.previous()}finally{this.confirmPaymentLoading.set(!1)}}async createOrderModel(){let n=this.cartService.cart(),e=await this.getAdreessFromStripeElement(),i=this.confirmationToken()?.payment_method_preview.card;if(!n?.id||!n.deliveryMethodId||!e||!i)throw new Error("Problem creating order");return{cartId:n.id,deliveryMethodId:n.deliveryMethodId,shippingAddress:e,paymentSummary:{last4Digits:+i.last4,brand:i.brand,expMonth:i.exp_month,expYear:i.exp_year}}}async getAdreessFromStripeElement(){let n=await this.addressElement?.getValue(),e=n?.value.address;return e?{name:n.value.name,line1:e.line1,line2:e.line2||void 0,city:e.city,state:e.state,postalCode:e.postal_code,country:e.country}:null}static \u0275fac=function(e){return new(e||t)};static \u0275cmp=E({type:t,selectors:[["app-checkout"]],decls:39,vars:11,consts:[["stepper",""],[1,"max-w-6xl","mx-auto"],[1,"flex","mt-32","gap-6"],[1,"w-3/4"],[1,"border","border-gray-200","shadow-sm","rounded-lg",3,"selectionChange","linear"],["label","Address",3,"completed"],["id","address-element"],[1,"flex","justify-end","mt-2"],[3,"change","checked"],[1,"flex","justify-between","mt-2"],["routerLink","/shop","mat-stroked-button","",1,"z-0"],["mat-flat-button","","matStepperNext","",1,"z-0",3,"disabled"],["label","Shipping",3,"completed"],[3,"deliveryComplete"],[1,"flex","justify-between","mt-6"],["mat-stroked-button","","matStepperPrevious",""],["mat-flat-button","","matStepperNext","",3,"disabled"],["label","Payment",3,"completed"],["id","payment-element"],["label","Confirmation"],[3,"confirmationToken"],["mat-flat-button","",3,"click","disabled"],["diameter","20"],[1,"w-1/4"]],template:function(e,i){if(e&1){let r=Mt();f(0,"div",1)(1,"div",2)(2,"div",3)(3,"mat-stepper",4,0),T("selectionChange",function(s){return i.onStepChange(s)}),f(5,"mat-step",5),O(6,"div",6),f(7,"div",7)(8,"mat-checkbox",8),T("change",function(s){return i.onSaveAddressCheckboxChange(s)}),_(9," Save as default address "),h()(),f(10,"div",9)(11,"button",10),_(12,"Continue Shopping"),h(),f(13,"button",11),_(14,"Next"),h()()(),f(15,"mat-step",12)(16,"app-checkout-delivery",13),T("deliveryComplete",function(s){return i.handleDeliveryChange(s)}),h(),f(17,"div",14)(18,"button",15),_(19,"Back"),h(),f(20,"button",16),_(21,"Next"),h()()(),f(22,"mat-step",17),O(23,"div",18),f(24,"div",14)(25,"button",15),_(26,"Back"),h(),f(27,"button",16),_(28,"Next"),h()()(),f(29,"mat-step",19),O(30,"app-checkout-review",20),f(31,"div",14)(32,"button",15),_(33,"Back"),h(),f(34,"button",21),T("click",function(){Te(r);let s=Re(4);return Ae(i.confirmPayment(s))}),N(35,MH,1,0,"mat-spinner",22)(36,TH,2,0,"span"),h()()()()(),f(37,"div",23),O(38,"app-order-summary"),h()()()}e&2&&(p(3),D("linear",!0),p(2),D("completed",i.completionStatus().address),p(3),D("checked",i.saveAddress),p(5),D("disabled",!i.completionStatus().address),p(2),D("completed",i.completionStatus().delivery),p(5),D("disabled",!i.completionStatus().delivery),p(2),D("completed",i.completionStatus().card),p(5),D("disabled",!i.completionStatus().card),p(3),D("confirmationToken",i.confirmationToken()),p(4),D("disabled",!i.confirmationToken||i.confirmPaymentLoading()),p(),P(i.confirmPaymentLoading()?35:36))},dependencies:[Za,hk,yy,xy,mk,fk,Pe,dt,Ik,Sy,$h,Gh,qh,Wh],encapsulation:2})};var AH=["*"];function RH(t,n){if(t&1&&(f(0,"mat-error"),_(1),h()),t&2){let e=S();p(),we("",e.label," is required")}}function OH(t,n){t&1&&(f(0,"mat-error"),_(1,"Email is invalid"),h())}function NH(t,n){t&1&&(f(0,"mat-error"),_(1,"Password must contain at least one uppercase letter"),h())}function PH(t,n){t&1&&(f(0,"mat-error"),_(1,"Password must contain at least one lowercase letter"),h())}function FH(t,n){t&1&&(f(0,"mat-error"),_(1,"Password must contain at least one number"),h())}function LH(t,n){t&1&&(f(0,"mat-error"),_(1,"Password must contain at least one special character"),h())}function jH(t,n){t&1&&(f(0,"mat-error"),_(1,"Password must be at least 8 characters long"),h())}function BH(t,n){t&1&&(f(0,"mat-error"),_(1,"Confirm password does not match with password"),h())}var Ja=class t{constructor(n){this.controlDir=n;this.controlDir.valueAccessor=this}label="";type="text";withIcon=!1;writeValue(n){}registerOnChange(n){}registerOnTouched(n){}get control(){return this.controlDir.control}static \u0275fac=function(e){return new(e||t)(ee(Cn,2))};static \u0275cmp=E({type:t,selectors:[["app-text-input"]],inputs:{label:"label",type:"type",withIcon:"withIcon"},ngContentSelectors:AH,decls:13,vars:14,consts:[["appearance","outline","subscriptSizing","dynamic",1,"w-full","mb-4"],["matInput","",1,"withIcon?","'pr-10!'",":","''",3,"formControl","type","placeholder"]],template:function(e,i){e&1&&(Ce(),f(0,"mat-form-field",0)(1,"mat-label"),_(2),h(),re(3),O(4,"input",1),N(5,RH,2,1,"mat-error"),N(6,OH,2,0,"mat-error"),N(7,NH,2,0,"mat-error"),N(8,PH,2,0,"mat-error"),N(9,FH,2,0,"mat-error"),N(10,LH,2,0,"mat-error"),N(11,jH,2,0,"mat-error"),N(12,BH,2,0,"mat-error"),h()),e&2&&(p(2),V(i.label),p(2),D("type",an(i.type))("placeholder",an(i.label))("formControl",i.control),p(),P(i.control.hasError("required")?5:-1),p(),P(i.control.hasError("email")?6:-1),p(),P(i.control.hasError("passwordUppercase")?7:-1),p(),P(i.control.hasError("passwordLowercase")?8:-1),p(),P(i.control.hasError("passwordNumber")?9:-1),p(),P(i.control.hasError("passwordSpecialChar")?10:-1),p(),P(i.control.hasError("passwordLength")?11:-1),p(),P(i.control.hasError("matchPassword")?12:-1))},dependencies:[za,ho,ii,ey,kr,qa,Ir,ly],encapsulation:2})};var Zh=class t{fb=d(eh);accountService=d(dn);router=d(Xe);activatedRoute=d(yn);passwordVisible=!1;returnUrl="/shop";constructor(){let n=this.activatedRoute.snapshot.queryParams.returnUrl;n&&(this.returnUrl=n)}loginForm=this.fb.group({email:["",[Jt.required,Jt.email]],password:["",Jt.required]});onSubmit(){this.loginForm.valid&&this.accountService.login(this.loginForm.value).subscribe({next:()=>{this.accountService.getUserInfo().subscribe(),this.router.navigateByUrl(this.returnUrl)},error:n=>console.error(n)})}togglePasswordVisibility(){this.passwordVisible=!this.passwordVisible}static \u0275fac=function(e){return new(e||t)};static \u0275cmp=E({type:t,selectors:[["app-login"]],decls:13,vars:5,consts:[[1,"max-w-lg","mx-auto","mt-32","p-8","bg-white!"],[3,"ngSubmit","formGroup"],[1,"text-center","mb-8"],[1,"text-3xl","font-semibold","text-blue-500"],["formControlName","email","label","Email","type","email"],["label","Password","formControlName","password",3,"type","withIcon"],[1,"absolute","inset-y-0","top-1","right-1","flex","items-center"],["mat-icon-button","","type","button",3,"click"],["mat-flat-button","","type","submit",1,"w-full","py-2","mt-4",3,"disabled"]],template:function(e,i){e&1&&(f(0,"mat-card",0)(1,"form",1),T("ngSubmit",function(){return i.onSubmit()}),f(2,"div",2)(3,"h1",3),_(4,"Login"),h()(),O(5,"app-text-input",4),f(6,"app-text-input",5)(7,"div",6)(8,"button",7),T("click",function(){return i.togglePasswordVisibility()}),f(9,"mat-icon"),_(10),h()()()(),f(11,"button",8),_(12,"Sign in"),h()()()),e&2&&(p(),D("formGroup",i.loginForm),p(5),D("type",i.passwordVisible?"text":"password")("withIcon",!0),p(4),V(i.passwordVisible?"visibility_off":"visibility"),p(),D("disabled",i.loginForm.invalid))},dependencies:[za,Ha,ii,Ba,Dr,ad,Zi,Pe,wt,Ja],encapsulation:2})};var Tr=class{static passwordUppercase(){return n=>/[A-Z]/.test(n.value)?null:{passwordUppercase:!0}}static passwordLowercase(){return n=>/[a-z]/.test(n.value)?null:{passwordLowercase:!0}}static passwordNumber(){return n=>/[0-9]/.test(n.value)?null:{passwordNumber:!0}}static passwordSpecialChar(){return n=>/[^A-Za-z0-9]/.test(n.value)?null:{passwordSpecialChar:!0}}static passwordLength(n=8){return e=>e.value&&e.value.length>=n?null:{passwordLength:!0}}static matchPassword(n){return e=>{let i=e.root.get(n);return i?i.value===e.value?null:{matchPassword:!0}:null}}};function VH(t,n){if(t&1&&(f(0,"li"),_(1),h()),t&2){let e=n.$implicit;p(),V(e)}}function HH(t,n){if(t&1&&(f(0,"div",11)(1,"ul",13),tt(2,VH,2,1,"li",null,vi),h()()),t&2){let e=S();p(2),nt(e.validationErrors())}}var Qh=class t{fb=d(eh);accountService=d(dn);router=d(Xe);snackbar=d(Rn);passwordVisible=!1;confirmPasswordVisible=!1;validationErrors=M([]);registerForm=this.fb.group({firstName:["",Jt.required],lastName:["",Jt.required],email:["",[Jt.required,Jt.email]],password:["",[Jt.required,Tr.passwordUppercase(),Tr.passwordLowercase(),Tr.passwordNumber(),Tr.passwordSpecialChar(),Tr.passwordLength(8)]],confirmPassword:["",[Jt.required,Tr.matchPassword("password")]]});onSubmit(){this.registerForm.valid&&this.accountService.register(this.registerForm.value).subscribe({next:()=>{this.snackbar.success("Registration successful! You can now log in."),this.router.navigateByUrl("/account/login")},error:n=>{console.error(n),this.validationErrors.set(n)}})}togglePasswordVisibility(){this.passwordVisible=!this.passwordVisible}toggleConfirmPasswordVisibility(){this.confirmPasswordVisible=!this.confirmPasswordVisible}static \u0275fac=function(e){return new(e||t)};static \u0275cmp=E({type:t,selectors:[["app-register"]],decls:21,vars:9,consts:[[1,"max-w-lg","mx-auto","mt-32","p-8","bg-white!"],[3,"ngSubmit","formGroup"],[1,"text-center","mb-8"],[1,"text-3xl","font-semibold","text-blue-500"],["label","First Name","formControlName","firstName"],["label","Last Name","formControlName","lastName"],["label","Email Address","formControlName","email","type","email"],["label","Password","formControlName","password",3,"type","withIcon"],[1,"absolute","inset-y-0","top-1","right-1","flex","items-center"],["mat-icon-button","","type","button",3,"click"],["label","Confirm Password","formControlName","confirmPassword",3,"type","withIcon"],[1,"text-red-600","mb-4","p-4","bg-red-100"],["mat-flat-button","","type","submit",1,"w-full","py-2",3,"disabled"],[1,"list-disc","px-3"]],template:function(e,i){e&1&&(f(0,"mat-card",0)(1,"form",1),T("ngSubmit",function(){return i.onSubmit()}),f(2,"div",2)(3,"h1",3),_(4,"Register"),h()(),O(5,"app-text-input",4)(6,"app-text-input",5)(7,"app-text-input",6),f(8,"app-text-input",7)(9,"div",8)(10,"button",9),T("click",function(){return i.togglePasswordVisibility()}),f(11,"mat-icon"),_(12),h()()()(),f(13,"app-text-input",10)(14,"div",8)(15,"button",9),T("click",function(){return i.toggleConfirmPasswordVisibility()}),f(16,"mat-icon"),_(17),h()()()(),N(18,HH,4,0,"div",11),f(19,"button",12),_(20,"Register"),h()()()),e&2&&(p(),D("formGroup",i.registerForm),p(7),D("type",i.passwordVisible?"text":"password")("withIcon",!0),p(4),V(i.passwordVisible?"visibility_off":"visibility"),p(),D("type",i.confirmPasswordVisible?"text":"password")("withIcon",!0),p(4),V(i.confirmPasswordVisible?"visibility_off":"visibility"),p(),P(i.validationErrors()&&i.validationErrors().length>0?18:-1),p(),D("disabled",i.registerForm.invalid))},dependencies:[za,Ha,ii,Ba,Dr,ad,Zi,Pe,wt,Ja],encapsulation:2})};var xd=(t,n)=>{let e=Dn(dn),i=Dn(Xe);return e.currentUser()?W(!0):e.getAuthState().pipe(Q(r=>r.isAuthenticated?!0:(i.navigate(["/account/login"],{queryParams:{returnUrl:n.url}}),!1)))};var Rk=(t,n)=>{let e=Dn(Rn),i=Dn(rt),r=Dn(Xe);return i.cart()==null||i.cart()?.items.length===0?(e.error("Please add items to your cart before proceeding to checkout."),r.navigateByUrl("/cart"),W(!1)):W(!0)};function UH(t,n){if(t&1&&(f(0,"section",0)(1,"div",1)(2,"h2",2),_(3,"Thanks for your fake order!"),h(),f(4,"p",3),_(5,"Your order "),f(6,"strong"),_(7),h(),_(8," will never be processed as this is a fake shop. We will not notify you of your order status."),h(),f(9,"div",4)(10,"dl",5)(11,"dt",3),_(12,"Date"),h(),f(13,"dd",6),_(14),Se(15,"date"),h()(),f(16,"dl",5)(17,"dt",3),_(18,"Payment method"),h(),f(19,"dd",6),_(20),Se(21,"paymentMethod"),h()(),f(22,"dl",5)(23,"dt",3),_(24,"Address"),h(),f(25,"dd",6),_(26),Se(27,"address"),h()(),f(28,"dl",5)(29,"dt",3),_(30,"Amount"),h(),f(31,"dd",6),_(32),Se(33,"currency"),h()()(),f(34,"div",7)(35,"button",8),_(36,"View your order"),h(),f(37,"button",9),_(38,"Continue shopping"),h()()()()),t&2){let e=n;p(7),we("#",e.id),p(7),V(Oe(15,7,e.orderDate)),p(6),V(Oe(21,9,e.paymentSummary)),p(6),V(Oe(27,11,e.shippingAddress)),p(6),V(Oe(33,13,e.total)),p(3),D("routerLink",Ln("/orders/",e.id))}}function zH(t,n){t&1&&(f(0,"section",0)(1,"div",1)(2,"h2",10),_(3,"Order processing, please wait..."),h(),f(4,"div",11)(5,"div",12),O(6,"mat-progress-spinner",13),f(7,"p",14),_(8,"Loading order..."),h(),f(9,"span"),_(10,"Your payment has been received, we are processing your order."),h()()(),f(11,"button",9),_(12,"Continue shopping"),h()()())}var Xh=class t{signalrService=d(bo);orderService=d(ri);ngOnDestroy(){this.orderService.orderCompleted.set(!1),this.signalrService.orderSignal.set(null)}static \u0275fac=function(e){return new(e||t)};static \u0275cmp=E({type:t,selectors:[["app-checkout-success"]],decls:2,vars:1,consts:[[1,"py-16"],[1,"mx-auto","max-w-2xl","px-4"],[1,"font-semibold","text-2xl","mb-2"],[1,"text-gray-500"],[1,"space-y-2","rounded-lg","border","border-gray-100","bg-gray-50","p-4","my-6"],[1,"flex","items-center","justify-between","gap-4"],[1,"font-medium"],[1,"flex","items-center","space-x-4"],["mat-flat-button","",3,"routerLink"],["routerLink","/shop","mat-stroked-button",""],[1,"font-semibold","text-2xl","mb-4"],[1,"rounded-lg","border","border-gray-100","bg-gray-50","p-6","mb-8"],[1,"flex","flex-col","items-center","justify-center","gap-2"],["mode","indeterminate","diameter","30"],[1,"text-xl"]],template:function(e,i){if(e&1&&N(0,UH,39,15,"section",0)(1,zH,13,0,"section",0),e&2){let r;P((r=i.signalrService.orderSignal())?0:1,r)}},dependencies:[dt,Pe,qh,Wh,rs,yo,Ut,xo],encapsulation:2})};var $H=(t,n)=>n.id;function GH(t,n){if(t&1&&(f(0,"tr",9)(1,"td",10),_(2),h(),f(3,"td",10),_(4),Se(5,"date"),h(),f(6,"td",10),_(7),Se(8,"currency"),h(),f(9,"td",10),_(10),h()()),t&2){let e=n.$implicit;D("routerLink",Ln("/orders/",e.id)),p(2),we("# ",e.id),p(2),V(el(5,6,e.orderDate,"medium")),p(3),V(Oe(8,9,e.total)),p(3),V(e.status)}}var Kh=class t{orderService=d(ri);orders=M([]);ngOnInit(){this.orderService.getOrdersForUser().subscribe({next:n=>this.orders.set(n),error:n=>console.error(n)})}static \u0275fac=function(e){return new(e||t)};static \u0275cmp=E({type:t,selectors:[["app-order"]],decls:19,vars:0,consts:[[1,"mx-auto","mt-32"],[1,"font-semibold","text-2xl","mb-6","text-center"],[1,"flex","flex-col"],[1,"w-full"],[1,"min-w-full","divide-y","divide-gray-200","cursor-pointer"],[1,"bg-gray-50"],[1,"text-gray-600","uppercase","text-left"],["scope","col",1,"px-6","py-3"],[1,"divide-y","divide-gray-200"],[1,"hover:bg-gray-100","text-left",3,"routerLink"],[1,"px-6","py-3"]],template:function(e,i){e&1&&(f(0,"div",0)(1,"h2",1),_(2,"My Orders"),h(),f(3,"div",2)(4,"div",3)(5,"table",4)(6,"thead",5)(7,"tr",6)(8,"th",7),_(9,"Order"),h(),f(10,"th",7),_(11,"Date"),h(),f(12,"th",7),_(13,"Total"),h(),f(14,"th",7),_(15,"Status"),h()()(),f(16,"tbody",8),tt(17,GH,11,11,"tr",9,$H),h()()()()()),e&2&&(p(17),nt(i.orders()))},dependencies:[dt,rs,Ut],encapsulation:2})};var WH=(t,n)=>n.productId;function qH(t,n){if(t&1&&(f(0,"tr")(1,"td",18)(2,"div",19),O(3,"img",20),f(4,"span"),_(5),h()()(),f(6,"td",21),_(7),h(),f(8,"td",22),_(9),Se(10,"currency"),h()()),t&2){let e=n.$implicit;p(3),D("src",an(e.pictureUrl),Qn),p(2),V(e.productName),p(2),we(" x",e.quantity," "),p(2),we(" ",Oe(10,5,e.price)," ")}}function YH(t,n){if(t&1&&(f(0,"mat-card",0)(1,"div",1)(2,"div",2)(3,"h2",3),_(4),h(),f(5,"button",4)(6,"mat-icon"),_(7,"arrow_back"),h(),_(8," Return to orders "),h()(),f(9,"div",5)(10,"div",6)(11,"h4",7),_(12,"Billing and delivery information"),h(),f(13,"dl")(14,"dt",8),_(15,"Shipping address"),h(),f(16,"dd",9),_(17),Se(18,"address"),h()(),f(19,"dl")(20,"dt",8),_(21,"Payment summary"),h(),f(22,"dd",9),_(23),Se(24,"paymentMethod"),h()()(),f(25,"div",6)(26,"h4",7),_(27,"Order details"),h(),f(28,"dl")(29,"dt",8),_(30,"Email Address"),h(),f(31,"dd",9),_(32),h()(),f(33,"dl")(34,"dt",8),_(35,"Order Status"),h(),f(36,"dd",9),_(37),h()(),f(38,"dl")(39,"dt",8),_(40,"Order Date"),h(),f(41,"dd",9),_(42),Se(43,"date"),h()()()(),f(44,"div",10)(45,"div",11)(46,"table",1)(47,"tbody",12),tt(48,qH,11,7,"tr",null,WH),h()()()(),f(50,"div",13)(51,"p",7),_(52,"Order Summary"),h(),f(53,"div",6)(54,"dl",14)(55,"dt",15),_(56,"Subtotal"),h(),f(57,"dd",8),_(58),Se(59,"currency"),h()(),f(60,"dl",14)(61,"dt",15),_(62,"Discount"),h(),f(63,"dd",16),_(64,"-$0.00"),h()(),f(65,"dl",14)(66,"dt",15),_(67,"Shipping"),h(),f(68,"dd",8),_(69),Se(70,"currency"),h()(),f(71,"dl",17)(72,"dt",15),_(73,"Total"),h(),f(74,"dd",8),_(75),Se(76,"currency"),h()()()()()()),t&2){let e,i,r,o,s,a,c,l,u,m,g=S();p(4),we("Order summary for order #",(e=g.order())==null?null:e.id),p(13),V(Oe(18,9,(i=g.order())==null?null:i.shippingAddress)),p(6),V(Oe(24,11,(r=g.order())==null?null:r.paymentSummary)),p(9),V((o=g.order())==null?null:o.buyerEmail),p(5),V((s=g.order())==null?null:s.status),p(5),V(el(43,13,(a=g.order())==null?null:a.orderDate,"medium")),p(6),nt((c=g.order())==null?null:c.orderItems),p(10),V(Oe(59,16,(l=g.order())==null?null:l.subtotal)),p(11),V(Oe(70,18,(u=g.order())==null?null:u.shippingPrice)),p(6),V(Oe(76,20,(m=g.order())==null?null:m.total))}}var Jh=class t{orderService=d(ri);activatedRoute=d(yn);order=M(null);ngOnInit(){this.loadOrder()}loadOrder(){let n=this.activatedRoute.snapshot.paramMap.get("id");n&&this.orderService.getOrderDetails(+n).subscribe({next:e=>this.order.set(e),error:e=>console.error(e)})}static \u0275fac=function(e){return new(e||t)};static \u0275cmp=E({type:t,selectors:[["app-order-details"]],decls:1,vars:1,consts:[[1,"p-8","shadow-md","max-w-6xl","mx-auto"],[1,"w-full"],[1,"flex","justify-between","items-center"],[1,"font-semibold","text-2xl","text-center"],["mat-stroked-button","","routerLink","/orders",1,"flex","items-center","gap-2"],[1,"mt-8","py-4","border-t","border-gray-200","grid","grid-cols-2"],[1,"space-y-2"],[1,"font-semibold"],[1,"font-medium"],[1,"mt-1","font-light"],[1,"mt-4","py-4"],[1,"border-y","border-gray-200"],[1,"divide-y","divide-gray-200"],[1,"space-y-4","py-2"],[1,"flex","items-center","justify-between","gap-4"],[1,"font-medium","text-gray-500"],[1,"font-medium","text-green-600"],[1,"flex","items-center","justify-between","gap-4","border-t","border-gray-200","pt-2"],[1,"py-4"],[1,"flex","items-center","gap-4"],["alt","product image",1,"w-12","h-12",3,"src"],[1,"py-4","text-center"],[1,"py-4","text-end"]],template:function(e,i){e&1&&N(0,YH,77,22,"mat-card",0),e&2&&P(i.order()?0:-1)},dependencies:[KE,Zi,Pe,wt,dt,Ut,rs,yo,xo],encapsulation:2})};var Ok=(t,n)=>{let e=Dn(ri),i=Dn(Xe);return e.orderCompleted()?!0:(i.navigateByUrl("/shop"),!1)};var Nk=[{path:"",component:gf},{path:"shop",component:ah},{path:"shop/:id",component:lh},{path:"cart",component:ph},{path:"checkout",component:Yh,canActivate:[xd,Rk]},{path:"checkout/success",component:Xh,canActivate:[xd,Ok]},{path:"orders",component:Kh,canActivate:[xd]},{path:"orders/:id",component:Jh,canActivate:[xd]},{path:"account/login",component:Zh},{path:"account/register",component:Qh},{path:"test-error",component:dh},{path:"not-found",component:uh},{path:"server-error",component:mh},{path:"**",redirectTo:"not-found",pathMatch:"full"}];var Pk=(t,n)=>{let e=Dn(Xe),i=Dn(Rn);return n(t).pipe(li(r=>{if(r)switch(r.status){case 400:if(r.error.errors){let s=[];for(let a in r.error.errors)r.error.errors[a]&&s.push(r.error.errors[a]);throw s.flat()}else i.error(r.error.title||r.error);break;case 401:i.error(r.error.title||r.error);break;case 404:e.navigateByUrl("/not-found");break;case 500:let o={state:{error:r.error}};e.navigateByUrl("/server-error",o);break;default:i.error("Something unexpected went wrong.");break}return Ao(()=>r)}))};var ec=class t{loading=M(!1);busyRequestCount=M(0);busy(){this.busyRequestCount.update(n=>n+1),this.loading.set(!0)}idle(){this.busyRequestCount.update(n=>Math.max(0,n-1)),this.busyRequestCount()<=0&&(this.busyRequestCount.set(0),this.loading.set(!1))}static \u0275fac=function(e){return new(e||t)};static \u0275prov=C({token:t,factory:t.\u0275fac,providedIn:"root"})};var Fk=(t,n)=>{let e=d(ec);return e.busy(),n(t).pipe(or(()=>e.idle()))};var ep=class t{cartService=d(rt);accountService=d(dn);signalrService=d(bo);init(){let n=localStorage.getItem("cart_id"),e=n?this.cartService.getCart(n):W(null);return Oo({cart:e,user:this.accountService.getUserInfo().pipe(st(i=>{i&&this.signalrService.createHubConnection()}))})}static \u0275fac=function(e){return new(e||t)};static \u0275prov=C({token:t,factory:t.\u0275fac,providedIn:"root"})};var Lk=(t,n)=>{let e=t.clone({withCredentials:!0});return n(e)};var jk={providers:[Dg(),fb(Nk),Lv(jv([Pk,Fk,Lk])),gm(async()=>{let t=d(ep);return i0(t.init()).finally(()=>{let n=document.getElementById("initial-splash");n&&n.remove()})})]};var Bk="mat-badge-content",ZH=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275cmp=E({type:t,selectors:[["ng-component"]],decls:0,vars:0,template:function(i,r){},styles:[`.mat-badge {
  position: relative;
}
.mat-badge.mat-badge {
  overflow: visible;
}

.mat-badge-content {
  position: absolute;
  text-align: center;
  display: inline-block;
  transition: transform 200ms ease-in-out;
  transform: scale(0.6);
  overflow: hidden;
  white-space: nowrap;
  text-overflow: ellipsis;
  box-sizing: border-box;
  pointer-events: none;
  background-color: var(--mat-badge-background-color, var(--mat-sys-error));
  color: var(--mat-badge-text-color, var(--mat-sys-on-error));
  font-family: var(--mat-badge-text-font, var(--mat-sys-label-small-font));
  font-weight: var(--mat-badge-text-weight, var(--mat-sys-label-small-weight));
  border-radius: var(--mat-badge-container-shape, var(--mat-sys-corner-full));
}
.mat-badge-above .mat-badge-content {
  bottom: 100%;
}
.mat-badge-below .mat-badge-content {
  top: 100%;
}
.mat-badge-before .mat-badge-content {
  right: 100%;
}
[dir=rtl] .mat-badge-before .mat-badge-content {
  right: auto;
  left: 100%;
}
.mat-badge-after .mat-badge-content {
  left: 100%;
}
[dir=rtl] .mat-badge-after .mat-badge-content {
  left: auto;
  right: 100%;
}
@media (forced-colors: active) {
  .mat-badge-content {
    outline: solid 1px;
    border-radius: 0;
  }
}

.mat-badge-disabled .mat-badge-content {
  background-color: var(--mat-badge-disabled-state-background-color, color-mix(in srgb, var(--mat-sys-error) 38%, transparent));
  color: var(--mat-badge-disabled-state-text-color, var(--mat-sys-on-error));
}

.mat-badge-hidden .mat-badge-content {
  display: none;
}

.ng-animate-disabled .mat-badge-content,
.mat-badge-content._mat-animation-noopable {
  transition: none;
}

.mat-badge-content.mat-badge-active {
  transform: none;
}

.mat-badge-small .mat-badge-content {
  width: var(--mat-badge-legacy-small-size-container-size, unset);
  height: var(--mat-badge-legacy-small-size-container-size, unset);
  min-width: var(--mat-badge-small-size-container-size, 6px);
  min-height: var(--mat-badge-small-size-container-size, 6px);
  line-height: var(--mat-badge-small-size-line-height, 6px);
  padding: var(--mat-badge-small-size-container-padding, 0);
  font-size: var(--mat-badge-small-size-text-size, 0);
  margin: var(--mat-badge-small-size-container-offset, -6px 0);
}
.mat-badge-small.mat-badge-overlap .mat-badge-content {
  margin: var(--mat-badge-small-size-container-overlap-offset, -6px);
}

.mat-badge-medium .mat-badge-content {
  width: var(--mat-badge-legacy-container-size, unset);
  height: var(--mat-badge-legacy-container-size, unset);
  min-width: var(--mat-badge-container-size, 16px);
  min-height: var(--mat-badge-container-size, 16px);
  line-height: var(--mat-badge-line-height, 16px);
  padding: var(--mat-badge-container-padding, 0 4px);
  font-size: var(--mat-badge-text-size, var(--mat-sys-label-small-size));
  margin: var(--mat-badge-container-offset, -12px 0);
}
.mat-badge-medium.mat-badge-overlap .mat-badge-content {
  margin: var(--mat-badge-container-overlap-offset, -12px);
}

.mat-badge-large .mat-badge-content {
  width: var(--mat-badge-legacy-large-size-container-size, unset);
  height: var(--mat-badge-legacy-large-size-container-size, unset);
  min-width: var(--mat-badge-large-size-container-size, 16px);
  min-height: var(--mat-badge-large-size-container-size, 16px);
  line-height: var(--mat-badge-large-size-line-height, 16px);
  padding: var(--mat-badge-large-size-container-padding, 0 4px);
  font-size: var(--mat-badge-large-size-text-size, var(--mat-sys-label-small-size));
  margin: var(--mat-badge-large-size-container-offset, -12px 0);
}
.mat-badge-large.mat-badge-overlap .mat-badge-content {
  margin: var(--mat-badge-large-size-container-overlap-offset, -12px);
}
`],encapsulation:2,changeDetection:0})}return t})(),Vk=(()=>{class t{_ngZone=d(H);_elementRef=d(L);_ariaDescriber=d(Sf);_renderer=d(xe);_animationsDisabled=ke();_idGenerator=d(Ie);get color(){return this._color}set color(e){this._setColor(e),this._color=e}_color="primary";overlap=!0;disabled=!1;position="above after";get content(){return this._content}set content(e){this._updateRenderedContent(e)}_content;get description(){return this._description}set description(e){this._updateDescription(e)}_description;size="medium";hidden=!1;_badgeElement;_inlineBadgeDescription;_isInitialized=!1;_interactivityChecker=d(Ia);_document=d(K);constructor(){let e=d(qe);e.load(ZH),e.load(Qi)}isAbove(){return this.position.indexOf("below")===-1}isAfter(){return this.position.indexOf("before")===-1}getBadgeElement(){return this._badgeElement}ngOnInit(){this._clearExistingBadges(),this.content&&!this._badgeElement&&(this._badgeElement=this._createBadgeElement(),this._updateRenderedContent(this.content)),this._isInitialized=!0}ngAfterViewInit(){}ngOnDestroy(){this._renderer.destroyNode&&(this._renderer.destroyNode(this._badgeElement),this._inlineBadgeDescription?.remove()),this._ariaDescriber.removeDescription(this._elementRef.nativeElement,this.description)}_isHostInteractive(){return this._interactivityChecker.isFocusable(this._elementRef.nativeElement,{ignoreVisibility:!0})}_createBadgeElement(){let e=this._renderer.createElement("span"),i="mat-badge-active";return e.setAttribute("id",this._idGenerator.getId("mat-badge-content-")),e.setAttribute("aria-hidden","true"),e.classList.add(Bk),this._animationsDisabled&&e.classList.add("_mat-animation-noopable"),this._elementRef.nativeElement.appendChild(e),typeof requestAnimationFrame=="function"&&!this._animationsDisabled?this._ngZone.runOutsideAngular(()=>{requestAnimationFrame(()=>{e.classList.add(i)})}):e.classList.add(i),e}_updateRenderedContent(e){let i=`${e??""}`.trim();this._isInitialized&&i&&!this._badgeElement&&(this._badgeElement=this._createBadgeElement()),this._badgeElement&&(this._badgeElement.textContent=i),this._content=i}_updateDescription(e){this._ariaDescriber.removeDescription(this._elementRef.nativeElement,this.description),(!e||this._isHostInteractive())&&this._removeInlineDescription(),this._description=e,this._isHostInteractive()?this._ariaDescriber.describe(this._elementRef.nativeElement,e):this._updateInlineDescription()}_updateInlineDescription(){this._inlineBadgeDescription||(this._inlineBadgeDescription=this._document.createElement("span"),this._inlineBadgeDescription.classList.add("cdk-visually-hidden")),this._inlineBadgeDescription.textContent=this.description,this._badgeElement?.appendChild(this._inlineBadgeDescription)}_removeInlineDescription(){this._inlineBadgeDescription?.remove(),this._inlineBadgeDescription=void 0}_setColor(e){let i=this._elementRef.nativeElement.classList;i.remove(`mat-badge-${this._color}`),e&&i.add(`mat-badge-${e}`)}_clearExistingBadges(){let e=this._elementRef.nativeElement.querySelectorAll(`:scope > .${Bk}`);for(let i of Array.from(e))i!==this._badgeElement&&i.remove()}static \u0275fac=function(i){return new(i||t)};static \u0275dir=A({type:t,selectors:[["","matBadge",""]],hostAttrs:[1,"mat-badge"],hostVars:20,hostBindings:function(i,r){i&2&&z("mat-badge-overlap",r.overlap)("mat-badge-above",r.isAbove())("mat-badge-below",!r.isAbove())("mat-badge-before",!r.isAfter())("mat-badge-after",r.isAfter())("mat-badge-small",r.size==="small")("mat-badge-medium",r.size==="medium")("mat-badge-large",r.size==="large")("mat-badge-hidden",r.hidden||!r.content)("mat-badge-disabled",r.disabled)},inputs:{color:[0,"matBadgeColor","color"],overlap:[2,"matBadgeOverlap","overlap",B],disabled:[2,"matBadgeDisabled","disabled",B],position:[0,"matBadgePosition","position"],content:[0,"matBadge","content"],description:[0,"matBadgeDescription","description"],size:[0,"matBadgeSize","size"],hidden:[2,"matBadgeHidden","hidden",B]}})}return t})();function QH(t,n){t&1&&Ht(0,"div",2)}var XH=new y("MAT_PROGRESS_BAR_DEFAULT_OPTIONS");var Uk=(()=>{class t{_elementRef=d(L);_ngZone=d(H);_changeDetectorRef=d(ve);_renderer=d(xe);_cleanupTransitionEnd;constructor(){let e=Hl(),i=d(XH,{optional:!0});this._isNoopAnimation=e==="di-disabled",e==="reduced-motion"&&this._elementRef.nativeElement.classList.add("mat-progress-bar-reduced-motion"),i&&(i.color&&(this.color=this._defaultColor=i.color),this.mode=i.mode||this.mode)}_isNoopAnimation;get color(){return this._color||this._defaultColor}set color(e){this._color=e}_color;_defaultColor="primary";get value(){return this._value}set value(e){this._value=Hk(e||0),this._changeDetectorRef.markForCheck()}_value=0;get bufferValue(){return this._bufferValue||0}set bufferValue(e){this._bufferValue=Hk(e||0),this._changeDetectorRef.markForCheck()}_bufferValue=0;animationEnd=new U;get mode(){return this._mode}set mode(e){this._mode=e,this._changeDetectorRef.markForCheck()}_mode="determinate";ngAfterViewInit(){this._ngZone.runOutsideAngular(()=>{this._cleanupTransitionEnd=this._renderer.listen(this._elementRef.nativeElement,"transitionend",this._transitionendHandler)})}ngOnDestroy(){this._cleanupTransitionEnd?.()}_getPrimaryBarTransform(){return`scaleX(${this._isIndeterminate()?1:this.value/100})`}_getBufferBarFlexBasis(){return`${this.mode==="buffer"?this.bufferValue:100}%`}_isIndeterminate(){return this.mode==="indeterminate"||this.mode==="query"}_transitionendHandler=e=>{this.animationEnd.observers.length===0||!e.target||!e.target.classList.contains("mdc-linear-progress__primary-bar")||(this.mode==="determinate"||this.mode==="buffer")&&this._ngZone.run(()=>this.animationEnd.next({value:this.value}))};static \u0275fac=function(i){return new(i||t)};static \u0275cmp=E({type:t,selectors:[["mat-progress-bar"]],hostAttrs:["role","progressbar","aria-valuemin","0","aria-valuemax","100","tabindex","-1",1,"mat-mdc-progress-bar","mdc-linear-progress"],hostVars:10,hostBindings:function(i,r){i&2&&(q("aria-valuenow",r._isIndeterminate()?null:r.value)("mode",r.mode),Tt("mat-"+r.color),z("_mat-animation-noopable",r._isNoopAnimation)("mdc-linear-progress--animation-ready",!r._isNoopAnimation)("mdc-linear-progress--indeterminate",r._isIndeterminate()))},inputs:{color:"color",value:[2,"value","value",lt],bufferValue:[2,"bufferValue","bufferValue",lt],mode:"mode"},outputs:{animationEnd:"animationEnd"},exportAs:["matProgressBar"],decls:7,vars:5,consts:[["aria-hidden","true",1,"mdc-linear-progress__buffer"],[1,"mdc-linear-progress__buffer-bar"],[1,"mdc-linear-progress__buffer-dots"],["aria-hidden","true",1,"mdc-linear-progress__bar","mdc-linear-progress__primary-bar"],[1,"mdc-linear-progress__bar-inner"],["aria-hidden","true",1,"mdc-linear-progress__bar","mdc-linear-progress__secondary-bar"]],template:function(i,r){i&1&&(Fe(0,"div",0),Ht(1,"div",1),N(2,QH,1,0,"div",2),Ge(),Fe(3,"div",3),Ht(4,"span",4),Ge(),Fe(5,"div",5),Ht(6,"span",4),Ge()),i&2&&(p(),In("flex-basis",r._getBufferBarFlexBasis()),p(),P(r.mode==="buffer"?2:-1),p(),In("transform",r._getPrimaryBarTransform()))},styles:[`.mat-mdc-progress-bar {
  --mat-progress-bar-animation-multiplier: 1;
  display: block;
  text-align: start;
}
.mat-mdc-progress-bar[mode=query] {
  transform: scaleX(-1);
}
.mat-mdc-progress-bar._mat-animation-noopable .mdc-linear-progress__buffer-dots,
.mat-mdc-progress-bar._mat-animation-noopable .mdc-linear-progress__primary-bar,
.mat-mdc-progress-bar._mat-animation-noopable .mdc-linear-progress__secondary-bar,
.mat-mdc-progress-bar._mat-animation-noopable .mdc-linear-progress__bar-inner.mdc-linear-progress__bar-inner {
  animation: none;
}
.mat-mdc-progress-bar._mat-animation-noopable .mdc-linear-progress__primary-bar,
.mat-mdc-progress-bar._mat-animation-noopable .mdc-linear-progress__buffer-bar {
  transition: transform 1ms;
}

.mat-progress-bar-reduced-motion {
  --mat-progress-bar-animation-multiplier: 2;
}

.mdc-linear-progress {
  position: relative;
  width: 100%;
  transform: translateZ(0);
  outline: 1px solid transparent;
  overflow-x: hidden;
  transition: opacity 250ms 0ms cubic-bezier(0.4, 0, 0.6, 1);
  height: max(var(--mat-progress-bar-track-height, 4px), var(--mat-progress-bar-active-indicator-height, 4px));
}
@media (forced-colors: active) {
  .mdc-linear-progress {
    outline-color: CanvasText;
  }
}

.mdc-linear-progress__bar {
  position: absolute;
  top: 0;
  bottom: 0;
  margin: auto 0;
  width: 100%;
  animation: none;
  transform-origin: top left;
  transition: transform 250ms 0ms cubic-bezier(0.4, 0, 0.6, 1);
  height: var(--mat-progress-bar-active-indicator-height, 4px);
}
.mdc-linear-progress--indeterminate .mdc-linear-progress__bar {
  transition: none;
}
[dir=rtl] .mdc-linear-progress__bar {
  right: 0;
  transform-origin: center right;
}

.mdc-linear-progress__bar-inner {
  display: inline-block;
  position: absolute;
  width: 100%;
  animation: none;
  border-top-style: solid;
  border-color: var(--mat-progress-bar-active-indicator-color, var(--mat-sys-primary));
  border-top-width: var(--mat-progress-bar-active-indicator-height, 4px);
}

.mdc-linear-progress__buffer {
  display: flex;
  position: absolute;
  top: 0;
  bottom: 0;
  margin: auto 0;
  width: 100%;
  overflow: hidden;
  height: var(--mat-progress-bar-track-height, 4px);
  border-radius: var(--mat-progress-bar-track-shape, var(--mat-sys-corner-none));
}

.mdc-linear-progress__buffer-dots {
  background-image: radial-gradient(circle, var(--mat-progress-bar-track-color, var(--mat-sys-surface-variant)) calc(var(--mat-progress-bar-track-height, 4px) / 2), transparent 0);
  background-repeat: repeat-x;
  background-size: calc(calc(var(--mat-progress-bar-track-height, 4px) / 2) * 5);
  background-position: left;
  flex: auto;
  transform: rotate(180deg);
  animation: mdc-linear-progress-buffering calc(250ms * var(--mat-progress-bar-animation-multiplier)) infinite linear;
}
@media (forced-colors: active) {
  .mdc-linear-progress__buffer-dots {
    background-color: ButtonBorder;
  }
}
[dir=rtl] .mdc-linear-progress__buffer-dots {
  animation: mdc-linear-progress-buffering-reverse calc(250ms * var(--mat-progress-bar-animation-multiplier)) infinite linear;
  transform: rotate(0);
}

.mdc-linear-progress__buffer-bar {
  flex: 0 1 100%;
  transition: flex-basis 250ms 0ms cubic-bezier(0.4, 0, 0.6, 1);
  background-color: var(--mat-progress-bar-track-color, var(--mat-sys-surface-variant));
}

.mdc-linear-progress__primary-bar {
  transform: scaleX(0);
}
.mdc-linear-progress--indeterminate .mdc-linear-progress__primary-bar {
  left: -145.166611%;
}
.mdc-linear-progress--indeterminate.mdc-linear-progress--animation-ready .mdc-linear-progress__primary-bar {
  animation: mdc-linear-progress-primary-indeterminate-translate calc(2s * var(--mat-progress-bar-animation-multiplier)) infinite linear;
}
.mdc-linear-progress--indeterminate.mdc-linear-progress--animation-ready .mdc-linear-progress__primary-bar > .mdc-linear-progress__bar-inner {
  animation: mdc-linear-progress-primary-indeterminate-scale calc(2s * var(--mat-progress-bar-animation-multiplier)) infinite linear;
}
[dir=rtl] .mdc-linear-progress.mdc-linear-progress--animation-ready .mdc-linear-progress__primary-bar {
  animation-name: mdc-linear-progress-primary-indeterminate-translate-reverse;
}
[dir=rtl] .mdc-linear-progress.mdc-linear-progress--indeterminate .mdc-linear-progress__primary-bar {
  right: -145.166611%;
  left: auto;
}

.mdc-linear-progress__secondary-bar {
  display: none;
}
.mdc-linear-progress--indeterminate .mdc-linear-progress__secondary-bar {
  left: -54.888891%;
  display: block;
}
.mdc-linear-progress--indeterminate.mdc-linear-progress--animation-ready .mdc-linear-progress__secondary-bar {
  animation: mdc-linear-progress-secondary-indeterminate-translate calc(2s * var(--mat-progress-bar-animation-multiplier)) infinite linear;
}
.mdc-linear-progress--indeterminate.mdc-linear-progress--animation-ready .mdc-linear-progress__secondary-bar > .mdc-linear-progress__bar-inner {
  animation: mdc-linear-progress-secondary-indeterminate-scale calc(2s * var(--mat-progress-bar-animation-multiplier)) infinite linear;
}
[dir=rtl] .mdc-linear-progress.mdc-linear-progress--animation-ready .mdc-linear-progress__secondary-bar {
  animation-name: mdc-linear-progress-secondary-indeterminate-translate-reverse;
}
[dir=rtl] .mdc-linear-progress.mdc-linear-progress--indeterminate .mdc-linear-progress__secondary-bar {
  right: -54.888891%;
  left: auto;
}

@keyframes mdc-linear-progress-buffering {
  from {
    transform: rotate(180deg) translateX(calc(var(--mat-progress-bar-track-height, 4px) * -2.5));
  }
}
@keyframes mdc-linear-progress-primary-indeterminate-translate {
  0% {
    transform: translateX(0);
  }
  20% {
    animation-timing-function: cubic-bezier(0.5, 0, 0.701732, 0.495819);
    transform: translateX(0);
  }
  59.15% {
    animation-timing-function: cubic-bezier(0.302435, 0.381352, 0.55, 0.956352);
    transform: translateX(83.67142%);
  }
  100% {
    transform: translateX(200.611057%);
  }
}
@keyframes mdc-linear-progress-primary-indeterminate-scale {
  0% {
    transform: scaleX(0.08);
  }
  36.65% {
    animation-timing-function: cubic-bezier(0.334731, 0.12482, 0.785844, 1);
    transform: scaleX(0.08);
  }
  69.15% {
    animation-timing-function: cubic-bezier(0.06, 0.11, 0.6, 1);
    transform: scaleX(0.661479);
  }
  100% {
    transform: scaleX(0.08);
  }
}
@keyframes mdc-linear-progress-secondary-indeterminate-translate {
  0% {
    animation-timing-function: cubic-bezier(0.15, 0, 0.515058, 0.409685);
    transform: translateX(0);
  }
  25% {
    animation-timing-function: cubic-bezier(0.31033, 0.284058, 0.8, 0.733712);
    transform: translateX(37.651913%);
  }
  48.35% {
    animation-timing-function: cubic-bezier(0.4, 0.627035, 0.6, 0.902026);
    transform: translateX(84.386165%);
  }
  100% {
    transform: translateX(160.277782%);
  }
}
@keyframes mdc-linear-progress-secondary-indeterminate-scale {
  0% {
    animation-timing-function: cubic-bezier(0.205028, 0.057051, 0.57661, 0.453971);
    transform: scaleX(0.08);
  }
  19.15% {
    animation-timing-function: cubic-bezier(0.152313, 0.196432, 0.648374, 1.004315);
    transform: scaleX(0.457104);
  }
  44.15% {
    animation-timing-function: cubic-bezier(0.257759, -0.003163, 0.211762, 1.38179);
    transform: scaleX(0.72796);
  }
  100% {
    transform: scaleX(0.08);
  }
}
@keyframes mdc-linear-progress-primary-indeterminate-translate-reverse {
  0% {
    transform: translateX(0);
  }
  20% {
    animation-timing-function: cubic-bezier(0.5, 0, 0.701732, 0.495819);
    transform: translateX(0);
  }
  59.15% {
    animation-timing-function: cubic-bezier(0.302435, 0.381352, 0.55, 0.956352);
    transform: translateX(-83.67142%);
  }
  100% {
    transform: translateX(-200.611057%);
  }
}
@keyframes mdc-linear-progress-secondary-indeterminate-translate-reverse {
  0% {
    animation-timing-function: cubic-bezier(0.15, 0, 0.515058, 0.409685);
    transform: translateX(0);
  }
  25% {
    animation-timing-function: cubic-bezier(0.31033, 0.284058, 0.8, 0.733712);
    transform: translateX(-37.651913%);
  }
  48.35% {
    animation-timing-function: cubic-bezier(0.4, 0.627035, 0.6, 0.902026);
    transform: translateX(-84.386165%);
  }
  100% {
    transform: translateX(-160.277782%);
  }
}
@keyframes mdc-linear-progress-buffering-reverse {
  from {
    transform: translateX(-10px);
  }
}
`],encapsulation:2,changeDetection:0})}return t})();function Hk(t,n=0,e=100){return Math.max(n,Math.min(e,t))}function KH(t,n){if(t&1&&(f(0,"button",6)(1,"mat-icon"),_(2,"arrow_drop_down"),h(),f(3,"span"),_(4),h()()),t&2){let e,i=S(),r=Re(11);D("matMenuTriggerFor",r),p(4),V((e=i.accountService.currentUser())==null?null:e.email)}}function JH(t,n){t&1&&(f(0,"button",11),_(1,"Login"),h(),f(2,"button",12),_(3,"Register"),h())}var tp=class t{busyService=d(ec);cartService=d(rt);accountService=d(dn);router=d(Xe);logout(){this.accountService.logout().subscribe({next:()=>{this.accountService.currentUser.set(null),this.router.navigateByUrl("/")},error:n=>console.error(n)})}static \u0275fac=function(e){return new(e||t)};static \u0275cmp=E({type:t,selectors:[["app-header"]],decls:28,vars:5,consts:[["menu","matMenu"],[1,"shadow-md","p-3","w-full","h-18","fixed","top-0","bg-white","z-50"],[1,"flex","items-center","justify-between","mx-auto"],["routerLink","/shop","src","images/logo.png","alt","app logo",1,"max-h-12","cursor-pointer"],[1,"flex","items-center","gap-3"],["matBadgeSize","large","routerLink","/cart","routerLinkActive","active",1,"custom-badge","mt-2","mr-2",3,"matBadge"],["mat-button","",3,"matMenuTriggerFor"],["mode","indeterminate",1,"fixed","top-18","z-50"],["mat-menu-item","","routerLink","/cart",1,"px-6!"],["mat-menu-item","","routerLink","/orders",1,"px-6!"],["mat-menu-item","",1,"px-6!",3,"click"],["mat-stroked-button","","routerLink","/account/login"],["mat-stroked-button","","routerLink","/account/register"]],template:function(e,i){e&1&&(f(0,"header",1)(1,"div",2),O(2,"img",3),f(3,"div",4)(4,"a",5)(5,"mat-icon"),_(6,"shopping_cart"),h()(),N(7,KH,5,2,"button",6)(8,JH,4,0),h()()(),O(9,"mat-progress-bar",7),f(10,"mat-menu",null,0)(12,"button",8)(13,"mat-icon"),_(14,"shopping_cart"),h(),f(15,"span"),_(16,"My Cart"),h()(),f(17,"button",9)(18,"mat-icon"),_(19,"history"),h(),f(20,"span"),_(21,"My Orders"),h()(),O(22,"mat-divider"),f(23,"button",10),T("click",function(){return i.logout()}),f(24,"mat-icon"),_(25,"logout"),h(),f(26,"span"),_(27,"Logout"),h()()()),e&2&&(p(4),D("matBadge",an(i.cartService.itemCount())),p(3),P(i.accountService.currentUser()?7:8),p(2),In("opacity",i.busyService.loading()?1:0))},dependencies:[wt,Pe,Vk,dt,mb,Uk,po,ih,dd,Cs],styles:[".custom-badge[_ngcontent-%COMP%]   .mat-badge-content[_ngcontent-%COMP%]{width:20px;height:20px;font-size:12px;line-height:20px}.custom-badge[_ngcontent-%COMP%]   .mat-icon[_ngcontent-%COMP%]{font-size:32px;width:32px;height:32px}a[_ngcontent-%COMP%]:hover{text-decoration:underline;text-underline-offset:4px}a.active[_ngcontent-%COMP%]{color:#3b82f6}"]})};var np=class t{static \u0275fac=function(e){return new(e||t)};static \u0275cmp=E({type:t,selectors:[["app-root"]],decls:3,vars:0,consts:[[1,"container","mt-24"]],template:function(e,i){e&1&&(O(0,"app-header"),f(1,"div",0),O(2,"router-outlet"),h())},dependencies:[Tl,tp],encapsulation:2})};Tv(np,jk).catch(t=>console.error(t));
