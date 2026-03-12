const fun=(vara)=>{
    console.log(`Hello ${vara}!`);
}
fun("World");


setTimeout(()=>{
    console.log("This is a delayed message.");
    clearInterval(timer);
},2000);

const timer=setInterval (() =>{
    console.log("This message will repeat every 1 seconds.");
},1000);

console.log(__dirname);
console.log(__filename);